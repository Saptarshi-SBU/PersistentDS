/*-----------------------------------------------------------------------------
 *
 *  Copyright(C): 2017
 *
 *  Storage Allocator for Persistent Storage
 *
 * ----------------------------------------------------------------------------*/

#ifndef _STORAGE_RESOURCE_H
#define _STORAGE_RESOURCE_H

#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <map>
#include <sstream>
#include <boost/intrusive/list.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>
#include "meta.pb.h"

using namespace boost::iostreams;
using namespace boost::intrusive;
using namespace boost::filesystem;

// Boost Iostreams Device
class StorageResource : public boost::iostreams::mapped_file {

    public :

       StorageResource(const std::string& filepath, size_type max_length) : mapped_file() {
          boost::iostreams::mapped_file_params params(filepath);
          params.flags = boost::iostreams::mapped_file::readwrite;
          params.offset = 0;
          params.length = max_length;
          path p(filepath);
          params.new_file_size = (exists(p) && is_regular_file(p)) ? 0 : max_length;
          open(params);
       }

      ~StorageResource() { close(); }
};

// Boost Iostreams stream
class CoreIO : public boost::iostreams::stream<boost::iostreams::mapped_file> {

    public :
      CoreIO(mapped_file  source) :
          boost::iostreams::stream<mapped_file> (source) {}

     ~CoreIO() { flush(); }

     void Read(off_t pos, char *buf, size_t size) {
        seekg(pos, beg);
        read(buf, size);
     }

     void Write(off_t pos, const char *buf, size_t size) {
        seekg(pos, beg);
        write(buf, size);
     }
};

class MappedRegion : public boost::enable_shared_from_this<MappedRegion> {

    public:
      MappedRegion(off_t start, size_t length):
          _base(start), _size(length) {}

      off_t _base;
      size_t _size;
};

#define RECORD_SIGNATURE (0xdeadface)
#define SPACEMAPREGIONSIZE (1024*1024)
#define METASLAB_SIZE (1024*1024*1024)

class MetaSlab : public MappedRegion {

    public:

      class SpaceMap {

          public:

            class SpaceMapRecord {

                public:

                // Note : required protobuf fields are set to default
                SpaceMapRecord() { magic = RECORD_SIGNATURE; size = 0; }

                //When you delegate the member initialization to another constructor,
                //there is an assumption that the other constructor initializes the
                //object completely, including all members
                SpaceMapRecord(off_t base, size_t size, db::spacemaprecord::RecordType alloc) {
                    rc.set_base(base);
                    rc.set_size(size);
                    rc.set_alloc(alloc);
                    size = 0;
                }

                void serialize(std::string &result) const {
                    rc.SerializeToString(&result);
                }

                void deserialize(std::string result) {
                    rc.ParseFromString(result);
                }

                const std::string DebugString(void) const {
                    return rc.ShortDebugString();
                }

               size_t Read(off_t start, boost::shared_ptr<CoreIO>& core) {
                    off_t pos = start;
                    size_t length = sizeof(unsigned int);
                    core->Read(pos, (char*)&magic, length);
                    if (magic == RECORD_SIGNATURE) {
                        pos+=length;
                        length = sizeof(size_t);
                        core->Read(pos, (char*)&size, length);
                        std::cout << pos << ":" << size << ":" << length << std::endl;
                        assert(size);
                        char *buf = new char[size];
                        pos+=length;
                        length = size;
                        core->Read(pos, buf, length);
                        deserialize(std::string(buf, size));
                        pos+=length;
                        delete buf;
                    } else
                        magic = 0;

                    return pos - start;
                }

                size_t Write(off_t start, boost::shared_ptr<CoreIO>& core) {
                    off_t pos = start;
                    size_t length = sizeof(unsigned int);
                    magic = RECORD_SIGNATURE;
                    core->Write(pos, (char*)&magic, length);
                    pos+=length;
                    length = sizeof(size_t);
                    std::string buf;
                    serialize(buf);
                    size = buf.size();
                    core->Write(pos, (char*)&size, length);
                    pos+=length;
                    length = size;
                    core->Write(pos, buf.c_str(), length);
                    pos+=length;
                    return pos - start;
                }

                unsigned int magic;
                size_t size; // record length
                db::spacemaprecord rc; // record

           };

           SpaceMap(off_t start, size_t size) : _start(start), _size(size) {}

           ~SpaceMap() {
               _free_map.clear();
               _alloc_map.clear();
           }

           // in-memory tree for spacemap allocations
           std::map<uint64_t, SpaceMap::SpaceMapRecord> _alloc_map;
           // in-memory tree for spacemap deallocations
           std::map<uint64_t, SpaceMap::SpaceMapRecord> _free_map;

           off_t _start; // space-map log region start
           size_t _size; // space-map log region size
      };


      MetaSlab(off_t start, size_t size, boost::shared_ptr<CoreIO> core) :
          MappedRegion(start, size),
         _log_size(SPACEMAPREGIONSIZE), _cursor(start), _cachedSize(size), _core(core) {

         _id = _base % _size;
         _spacemap.reset(new SpaceMap(_base, _log_size));

         // Update from on-disk Log
         while (_cursor < (start + _log_size)) {
            SpaceMap::SpaceMapRecord rec;
            auto n = rec.Read(_cursor, _core);
            BOOST_LOG_TRIVIAL(info) << "Space-Map record size :" << n;
            if (0 == n)
                break;
            if (rec.rc.alloc() == db::spacemaprecord::ALLOCATE) {
               _spacemap->_alloc_map[rec.rc.base()] = rec;
                auto it = _spacemap->_free_map.find(rec.rc.base());
                if (it != _spacemap->_free_map.end())
                    _spacemap->_free_map.erase(it);
               _cachedSize-=rec.rc.size();
            } else {
               _spacemap->_free_map[rec.rc.base()] = rec;
                auto it = _spacemap->_alloc_map.find(rec.rc.base());
                if (it != _spacemap->_alloc_map.end())
                    _spacemap->_alloc_map.erase(it);
                if (rec.rc.alloc() == db::spacemaprecord::DEALLOCATE)
                   _cachedSize+=rec.rc.size();
            }
           _cursor+=n;
            BOOST_LOG_TRIVIAL(info) << "On-Disk Space-Map Record " << rec.DebugString();
         }

         // else Initialize New Map
         if (_cursor == start) {
             SpaceMap::SpaceMapRecord rec(_base + _log_size, size, db::spacemaprecord::FREE);
            _cursor+=rec.Write(_cursor, _core);
            _spacemap->_free_map[rec.rc.base()] = rec;
             BOOST_LOG_TRIVIAL(info) << "Creating Space-Map Record" << rec.DebugString();
         }

         BOOST_LOG_TRIVIAL(info) << "MetaSlab Avaliable Space " << _cachedSize;
      }

      ~MetaSlab() {
          _spacemap.reset();
          _core.reset();
      }

      // Get from in-memory tree
      std::pair<off_t, size_t>Allocate(std::string::size_type size) {
         for (auto it : _spacemap->_free_map) {
            if (it.second.rc.size() >= size) {
               // Create New Allocation Record
               SpaceMap::SpaceMapRecord rec(it.second.rc.base(), size, db::spacemaprecord::ALLOCATE);
              _cursor+=rec.Write(_cursor, _core);
               // Update tree
              _spacemap->_free_map.erase(rec.rc.base());
              _spacemap->_alloc_map[rec.rc.base()] = rec;
               std::cout << rec.DebugString() << std::endl;

               // Create New DeAllocation record
               auto new_pos = it.second.rc.base() + size;
               auto new_size = it.second.rc.size() - size;
               SpaceMap::SpaceMapRecord new_rec(it.second.rc.base() + size,
                       it.second.rc.size() - size, db::spacemaprecord::FREE);
              _cursor+=new_rec.Write(_cursor, _core);
               // Update tree
              _spacemap->_free_map[new_pos] = new_rec;
               std::cout << new_rec.DebugString() << std::endl;

              _cachedSize-=size;
               return std::pair<off_t, size_t>(rec.rc.base(), rec.rc.size());
             }
         }
         BOOST_LOG_TRIVIAL(error) << "SpaceMap First Fit failed to find any entry";
         throw std::bad_alloc();
      }

      void DeAllocate(off_t start, size_t size) {
         // Free to in-memory tree
         SpaceMap::SpaceMapRecord rec(start, size, db::spacemaprecord::DEALLOCATE);
         // Update Log
         _cursor+=rec.Write(_cursor, _core);
         // Update tree
         _spacemap->_free_map[rec.rc.base()] = rec;
         _spacemap->_alloc_map.erase(rec.rc.base());
         _cachedSize+=size;
         std::cout << rec.DebugString() << std::endl;
      }

      unsigned int _id;  // meta-slab id
      const size_t _log_size; // log-region reserved for spacemap
      size_t _cachedSize;
      off_t _cursor; // cursor for the log region
      boost::shared_ptr<CoreIO> _core;
      boost::shared_ptr<SpaceMap> _spacemap;
};

class StorageAllocator
{
    private:

        std::list<boost::shared_ptr<MetaSlab>> _mslabs;

    public:
        StorageAllocator(StorageResource& sink, const off_t base, size_t size) {
           if (size < METASLAB_SIZE)
               throw ("Invalid File Size");

           int nr = size/METASLAB_SIZE;
           for (int i = 0; i < nr; i++) {
              boost::shared_ptr<MetaSlab> slab;
              boost::shared_ptr<CoreIO> core;
              // Write Stream Per MetaSlab
              core.reset(new CoreIO(sink));
              slab.reset(new MetaSlab(base + i*METASLAB_SIZE, METASLAB_SIZE, core));
             _mslabs.push_back(slab);
           }

           std::cout << "Total Meta-Slabs " << _mslabs.size() << std::endl;
        }

       ~StorageAllocator() {
          _mslabs.clear();
       }

       std::pair<off_t, size_t> Allocate(std::string::size_type n, void* hint = 0) {
          BOOST_LOG_TRIVIAL(info) << __func__ << " request size: " << n;
          for (auto it : _mslabs) {
             if (it->_cachedSize > n) {
                auto result = it->Allocate(n);
                if (result.second)
                   return result;
                }
                assert(0);
          }
          BOOST_LOG_TRIVIAL(error) << "Metaslab: No Free region";
          throw std::bad_alloc();
       }

       void DeAllocate(off_t start, size_t size) {
          BOOST_LOG_TRIVIAL(info) << __func__ << " freed size: " << size;
          for (auto it : _mslabs) {
              if ((it->_base < start) &&
                  (it->_base + it->_size) > (start + size)) {
                  it->DeAllocate(start, size);
                  break;
              }
          }
       }
};

void TestIODevice(void) {
    StorageResource sink(std::string("log.txt"), 4096);
    CoreIO core(sink);
    size_t size = 0xdeadbeef;
    core.Write(0, (char*)&size, sizeof(size));
    size = 0;
    core.Read(0, (char*)&size, sizeof(size));
    std::cout << std::hex << size << std::endl;
    std::cout << core.tellg() << std::endl;
}

#endif
