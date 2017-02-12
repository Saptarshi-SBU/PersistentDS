#include "storage_allocator.hpp"

#define PAGE_SIZE 4096
#define HEADER_SIGNATURE (0xdeadbeef)
#define MAX_READ (1024*1024)

template<class T>
class LinkListNode {

    public:

        enum OpType {
            APPEND,
            ERASE
        };

        struct Data {
            T _value;
            off_t _phys_curr;
            off_t _phys_prev;
            off_t _phys_next;
            OpType _op;
        }data;

        LinkListNode(T value) {
            bzero((char*)&data, sizeof(struct Data));
            data._value = value;
            data._op = APPEND;
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
           ss << " value: " << data._value << " off_curr: " << data._phys_curr;
           ss << " off_prev: " << data._phys_prev << " off_next: " << data._phys_next;
           ss << " op: " << data._op;
           return ss.str();
        }
};

//The PersistentLink List manages its own storage space
//allocated from the Storage Allocator
template<class T, class IO, class Allocator>
class PersistentLinkList {

    private:

       boost::shared_ptr<IO> _core;
       boost::shared_ptr<Allocator> _allocator;

    public:

       // Signature
       class Header {

           private :

               struct Meta {
                    unsigned int _magic;
                    unsigned int _version;
                    off_t _phys_curr;
                    off_t _phys_next;
                    unsigned int _nr_elements;
               };

           public:

               void serialize(char *buf, size_t size) const {
                  assert (sizeof(struct Meta) == size);
                  std::copy((char*)&_meta._magic, (char*)&_meta._magic + size, buf);
               }

               void deserialize(const char *buf, size_t size) {
                  assert (sizeof(struct Meta) == size);
                  if (HEADER_SIGNATURE == *(reinterpret_cast<const unsigned int*>(buf)))
                      std::copy(buf, buf + sizeof(struct Meta), (char*)&_meta._magic);
               }

               const std::string DebugString(void) const {
                  std::ostringstream ss;
                  ss << " magic: " << _meta._magic << " version: " << _meta._version;
                  ss << " off_curr: " << _meta._phys_curr << " off_next: " << _meta._phys_next;
                  ss << " nr_elements: " << _meta._nr_elements;
                  return ss.str();
               }

               Header(PersistentLinkList<T, IO, Allocator>& pList) : _parent(pList) {

                    // Initialize from On-Disk
                    off_t pos  = SPACEMAPREGIONSIZE;
                    const size_t size = sizeof(struct Meta);
                    bzero((char*)&_meta, size);
                    size_t n = 0;
                    while (n < MAX_READ && (_meta._magic != HEADER_SIGNATURE)) {
                        char rbuf[size];
                       _parent._core->Read(pos, rbuf, size);
                        deserialize(rbuf, size);
                        pos+=size;
                        n+=size;
                    };

                    // No entry on-disk; else initialize In-Memory
                    if (_meta._magic == HEADER_SIGNATURE)
                       BOOST_LOG_TRIVIAL(info) << "Header version on disk: " << _meta._version;
                    else {
                        _meta._magic = HEADER_SIGNATURE;
                         auto mem = _parent._allocator->Allocate(size);
                        _meta._phys_curr = mem.first;
                         char *wbuf = new char[size];
                         serialize(wbuf, size);
                        _parent._core->Write(_meta._phys_curr, wbuf, size);
                    }
               }

              ~Header() {
               _parent._core->flush();
              }

              struct Meta _meta;  //on-disk
              PersistentLinkList<T, IO, Allocator>& _parent; //ref to outer class
       };

       class Iterator {

          boost::shared_ptr<LinkListNode<T>> _curr;

          public :

          boost::shared_ptr<LinkListNode<T>> get(void) const {
              return _curr;
          }

          bool hasNext(void) const {
              return (_curr->data._phys_next) ? true : false;
          }

          Iterator Next(boost::shared_ptr<IO> &core) const {
              const off_t pos = _curr->data._phys_next;
              auto node = boost::shared_ptr<LinkListNode<T>>(new LinkListNode<T>());
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

       boost::shared_ptr<struct Header> _header;
       boost::shared_ptr<LinkListNode<T>> _head;
       std::list<boost::shared_ptr<LinkListNode<T>>> _list;

       void BuildList(void) {

           if (!_header->_meta._phys_next) {
              BOOST_LOG_TRIVIAL(error) << "No List entry found!";
              return;
           }

           // Create Head
          _head.reset(new LinkListNode<T>());
           const size_t size = sizeof(_head->data);
           char rbuf[size];
          _core->Read(_header->_meta._phys_next, rbuf, size);
          _head->deserialize(rbuf, size);

           // Next populate all entries
           size_t count = 0;
           Iterator it(_head);
           do {
               auto node = it.get();
               _list.push_back(node);
               count++;
               if (!it.hasNext())
                   break;
               it = it.Next(_core);
           } while (count < _header->_meta._nr_elements);
           assert (count == _header->_meta._nr_elements);
           //std::cout << "node entry: " << node->DebugString() << std::endl;
           return;
       }

       PersistentLinkList(boost::shared_ptr<IO> core, boost::shared_ptr<Allocator> alloc) : _core(core), _allocator(alloc) {
           _header.reset(new Header(*this));
           BOOST_LOG_TRIVIAL(info) << _header->DebugString();
           if (_header->_meta._nr_elements)
              BuildList();
       }

       void push_back(const T& x) {

           const size_t dsize = sizeof(_header->_meta);
           char dbuf[dsize];

           // Allocate Node
           auto node = boost::shared_ptr<LinkListNode<T>> (new LinkListNode<T>(x));
           const size_t size = sizeof(node->data);
           char pbuf[size];
           char qbuf[size];

           auto mem = _allocator->Allocate(size);
           node->data._phys_curr = mem.first;

           if (!_list.empty()) {
              auto tail = _list.rbegin();
              node->data._phys_prev = (*tail)->data._phys_curr;
              (*tail)->data._phys_next = node->data._phys_curr;
             _header->_meta._nr_elements++;
             _header->serialize(dbuf, dsize);
              (*tail)->serialize(qbuf, size);
              node->serialize(pbuf, size);
             _core->Write((*tail)->data._phys_curr, qbuf, size);
           } else {
             _header->_meta._phys_next = node->data._phys_curr;
             _header->_meta._nr_elements++;
             _header->serialize(dbuf, dsize);
              node->serialize(pbuf, size);
           }

           _core->Write(node->data._phys_curr, pbuf, size);
           _core->Write(_header->_meta._phys_curr, dbuf, dsize);
           _list.push_back(node);
            BOOST_LOG_TRIVIAL(info) << "node pushed: " << node->DebugString();
       }

       void pop_back(void) {

           if (!_header->_meta._phys_next) {
              BOOST_LOG_TRIVIAL(info) << "No List entry found!";
              return;
           }

           boost::shared_ptr<LinkListNode<T>> tail;
           for (auto iter = _list.rbegin(); iter != _list.rend(); iter++) {
              if ((*iter)->data._op == LinkListNode<T>::APPEND) {
                 tail = (*iter);
                 break;
              }
           }

           // Note we do not update header
           if (tail) {
              const size_t size = sizeof(tail->data);
              char buf[size];
              tail->data._op = LinkListNode<T>::ERASE;
              tail->serialize(buf, size);
             _core->Write(tail->data._phys_curr, buf, size);
              BOOST_LOG_TRIVIAL(info) << "node removed: " << tail->DebugString();
           }
       }

       void clear(void) {

          if (!_header->_meta._phys_next) {
             BOOST_LOG_TRIVIAL(info) << "No List entry found!";
             return;
          }

          // Compute Region Size to Free;
          const size_t size = sizeof(_head->data);
          const size_t total_size = _header->_meta._nr_elements * size;

          const size_t hsize = sizeof(_header->_meta);
          char buf[hsize];

          // Update Header
          auto next = _header->_meta._phys_next;
          _header->_meta._phys_next = 0;
          _header->_meta._nr_elements = 0;
          _header->serialize(buf, hsize);
          _core->Write(_header->_meta._phys_curr, buf, hsize);

          //Punch Holes to Clear Region
          char *zeroregion = new char [total_size];
          _core->Write(next, zeroregion, total_size);
          delete zeroregion;

          // Free the Region
          _allocator->DeAllocate(next, total_size);
          delete zeroregion;

          BOOST_LOG_TRIVIAL(info) << "List Cleared Size: " << total_size;

          // Finally Purge the List
          _list.clear();
       }

       void print(void) const {
          BOOST_LOG_TRIVIAL(info) << "------Linked List Dump--------";
          for (auto &i : _list)
             if(i->data._op == LinkListNode<T>::APPEND)
                std::cout << i->DebugString() << std::endl;
       }
};

void TestLinkListNode(void) {
    auto node = boost::shared_ptr<LinkListNode<std::string>>(new LinkListNode<std::string>("Hello"));
    std::cout << node->DebugString() << std::endl;
    PersistentLinkList<std::string, CoreIO, StorageAllocator>::Iterator it(node);
}

void TestPersistentLinkList(void) {
    size_t size = 1024*1024*1024;
    StorageResource sink(std::string("log.txt"), size);
    auto io = boost::shared_ptr<CoreIO>(new CoreIO(sink));;
    auto allocator = boost::shared_ptr<StorageAllocator>(new StorageAllocator(sink, 0, size));
    typedef PersistentLinkList<int, CoreIO, StorageAllocator> PMemLinkList;
    auto pList = boost::shared_ptr<PMemLinkList>(new PMemLinkList(io, allocator));
    pList->push_back(1);
    pList->push_back(2);
    pList->push_back(3);
    pList->pop_back();
}
