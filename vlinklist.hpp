/*-----------------------------------------------------------------------------
 *
 *  Copyright(C): 2017 Saptarshi Sen
 *
 *  Persistent LinkList with Support for Snapshots
 *
 * ----------------------------------------------------------------------------*/
#include <map>

#include "storage_allocator.hpp"
#include "registry.hpp"
#include "meta.pb.h"

template<class T>
class LinkListNode {

    public:

        struct Data {
            T _value;
            off_t _phys_curr;
            off_t _phys_birth;
        }data;

        LinkListNode(T value) {
            bzero((char*)&data, sizeof(struct Data));
            data._value = value;
        }

        LinkListNode() {
            bzero((char*)&data, sizeof(struct Data));
        }

        void serialize(char *buf, size_t size) const {
           assert (sizeof(struct Data) == size);
           std::copy((char*)&data._value, (char*)&data._value + size, buf);
        }

        void deserialize(const char *buf, size_t size) {
           assert (sizeof(struct Data) == size);
           std::copy(buf, buf + sizeof(struct Data), (char*)&data._value);
        }

        const std::string DebugString(void) const {
           std::ostringstream ss;
           ss << " value: " << data._value ;
           ss << " off_curr: " << data._phys_curr << " birth: " << data._phys_birth;
           return ss.str();
        }
};

template<class T, class IO, class Allocator>
class PersistentLinkList {

    public:

       class Iterator {

          boost::shared_ptr<LinkListNode<T>> _curr;

          public :

          boost::shared_ptr<LinkListNode<T>> get(void) const {
             return _curr;
          }

          Iterator Next(boost::shared_ptr<IO> &core) const {
             const off_t pos = _curr->data._phys_curr + sizeof(_curr->data);
             auto node =
                 boost::shared_ptr<LinkListNode<T>>(new LinkListNode<T>());
             const size_t size = sizeof(node->data);
             char rbuf[size];
             core->Read(pos, rbuf, size);
             node->deserialize(rbuf, size);
             return Iterator(node);
          }

          Iterator(boost::shared_ptr<LinkListNode<T>> node) : _curr(node) {}

          Iterator operator=(const Iterator& node) {
             (*this)._curr = node._curr;
              return *this;
          }

       };

       void BuildList(void) {

           if (0 == preg.phys_next()) {
              BOOST_LOG_TRIVIAL(error) << __func__ << " List is empty!";
              return;
           }

           // Create Head
          _head.reset(new LinkListNode<T>());
           const size_t size = sizeof(_head->data);
           char rbuf[size];
          _core->Read(preg.phys_next(), rbuf, size);
          _head->deserialize(rbuf, size);

           // Next populate all entries
           size_t count = 0;
           Iterator it(_head);
           do {
               auto node = it.get();
              _list.push_back(node);
               if (node->data._phys_birth)
                   _map[node->data._value] = node;
               else {
                   assert(_map.find(node->data._value) != _map.end());
                   _map.erase(node->data._value);
               }
               BOOST_LOG_TRIVIAL(debug) << "node entry: " << node->DebugString();
               count++;
               if (count == preg.nr_elements())
                   break;
               it = it.Next(_core);
           } while (1);
       }

       void push_back(const T& x, bool hole=false) {

           // Allocate Node
           auto node =
               boost::shared_ptr<LinkListNode<T>> (new LinkListNode<T>(x));
           const size_t size = sizeof(node->data);
           char pbuf[size];

           auto mem = _allocator->Allocate(size);
           node->data._phys_curr = mem.first;
           if (hole)
               node->data._phys_birth = 0;
           else
               node->data._phys_birth = mem.first;
           node->serialize(pbuf, size);
          _core->Write(node->data._phys_curr, pbuf, size);

           if(_list.empty())
              preg.set_phys_next(node->data._phys_curr);
           auto nr = preg.nr_elements();
           preg.set_nr_elements(nr + 1);
          _greg->update(preg);

          _list.push_back(node);
          _map[node->data._value] = node;

           BOOST_LOG_TRIVIAL(debug) << "node pushed: " << node->DebugString();
       }

       void pop_back(void) {

           if (_list.empty()) {
              BOOST_LOG_TRIVIAL(error) << __func__ << " List is empty!";
              return;
           }

           int snapitems = _greg->GetSnapElements(preg.key());
           BOOST_LOG_TRIVIAL(debug) << "Snap items : " << snapitems;

           typename std::list<boost::shared_ptr<LinkListNode<T>>>::iterator iter = _list.begin();
           for (int i = 0; i < snapitems - 1; i++) { if (iter != _list.end()) iter++; }
           BOOST_LOG_TRIVIAL(debug) << "Max item : " << (*iter)->DebugString();

           //for (auto riter = _list.rbegin(); riter != iter; riter++) {
           for (auto riter = _list.rbegin(); riter != _list.rend(); riter++) {
              if ((*riter)->data._value == (*iter)->data._value) {
                  BOOST_LOG_TRIVIAL(error) << "Cannot pop items which are snapshotted";
                  break;
              } else if (((*riter)->data._phys_birth > 0) &&
                 (_map.find((*riter)->data._value) != _map.end())) {
                  push_back((*riter)->data._value, true);
                 _map.erase((*riter)->data._value);
                  break;
              }
           }
       }

       void clear(void) {

          if (!preg.phys_next()) {
             _greg->remove(preg.key());
              return;
          }

          if (_greg->GetSnapElements(preg.key())) {
              BOOST_LOG_TRIVIAL(error) << "Cannot Clear Parent having snapshots";
              return;
          }

          // Compute Region Size to Free;
          const size_t size = sizeof(_head->data);
          const size_t total_size = preg.nr_elements() * size;

          // Punch Holes to Clear Region
          char *zeroregion = new char [total_size];
         _core->Write(preg.phys_next(), zeroregion, total_size);
          delete zeroregion;

          // Free the Region
         _allocator->DeAllocate(preg.phys_next(), total_size);

         _head.reset();

          // Finally Purge the List
         _list.clear();
          BOOST_LOG_TRIVIAL(debug) << "List Cleared Size: " << total_size;

          // Remove entry from registry
         _greg->remove(preg.key());
       }

       void print(void) const {
          BOOST_LOG_TRIVIAL(info) << "------Linked List Dump--------";
          for (auto &i : _map)
                std::cout << i.second->DebugString() << std::endl;
       }

       PersistentLinkList(std::string id,
           boost::shared_ptr<Registry<IO, Allocator>> reg,
           boost::shared_ptr<IO> core, boost::shared_ptr<Allocator> alloc)
           : _core(core), _allocator(alloc), _greg(reg) {

           auto key = boost::hash_value(id);
           if (!_greg->find(key, preg)) {
              _greg->insert(key);
              assert(_greg->find(key, preg));
              BOOST_LOG_TRIVIAL(info) << "Created Registry entry for id " << id;
           } else {
              BOOST_LOG_TRIVIAL(debug) << "Registry record found " << preg.ShortDebugString();
           }

           if (preg.nr_elements()) {
              BuildList();
           }
       }

    private:

       boost::shared_ptr<IO> _core;
       boost::shared_ptr<Allocator> _allocator;

       //Global registry
       boost::shared_ptr<Registry<IO, Allocator>> _greg;

       // Registry record
       db::registryrecord preg;

       // List Head
       boost::shared_ptr<LinkListNode<T>> _head;

       // In-memory copy
       std::list<boost::shared_ptr<LinkListNode<T>>> _list;

       // In-memory copy for live-objects
       std::map<T, boost::shared_ptr<LinkListNode<T>>> _map;

};

void TestLinkListNode(void) {
    auto node = boost::shared_ptr<LinkListNode<std::string>>(new LinkListNode<std::string>("Hello"));
    std::cout << node->DebugString() << std::endl;
    PersistentLinkList<std::string, CoreIO, StorageAllocator>::Iterator it(node);
}

void TestPersistentLinkList(void) {
    #define DEFAULTKEY "TEST"
    #define MAX_READ (1024*1024)
    size_t size = 1024*1024*1024;
    StorageResource sink(std::string("log.txt"), size);
    auto io = boost::shared_ptr<CoreIO>(new CoreIO(sink));;
    auto allocator = boost::shared_ptr<StorageAllocator>(new StorageAllocator(sink, 0, size));
    typedef Registry<CoreIO, StorageAllocator> GlobalReg;
    auto reg = boost::shared_ptr<GlobalReg>(new GlobalReg(MAX_READ, io, allocator));
    typedef PersistentLinkList<int, CoreIO, StorageAllocator> PMemLinkList;
    auto pList = boost::shared_ptr<PMemLinkList>
        (new PMemLinkList(std::string(DEFAULTKEY), reg, io, allocator));
    pList->push_back(1);
    pList->push_back(2);
    pList->push_back(3);
    pList->pop_back();
}
