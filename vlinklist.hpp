/*-----------------------------------------------------------------------------
 *
 *  Copyright(C): 2017
 *
 *  Persistent LinkList Implementation
 *
 * ----------------------------------------------------------------------------*/

#include "storage_allocator.hpp"
#include "registry.hpp"
#include "meta.pb.h"

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
           ss << " off_prev: " << data._phys_prev;
           ss << " off_next: " << data._phys_next << " op: " << data._op;
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

          bool hasNext(void) const {
              return (_curr->data._phys_next) ? true : false;
          }

          Iterator Next(boost::shared_ptr<IO> &core) const {
              const off_t pos = _curr->data._phys_next;
              auto node =
                  boost::shared_ptr<LinkListNode<T>>(new LinkListNode<T>());
              const size_t size = sizeof(node->data);
              char rbuf[size];
              core->Read(pos, rbuf, size);
              node->deserialize(rbuf, size);
              return Iterator(node);
          }

          Iterator(boost::shared_ptr<LinkListNode<T>> node) :
              _curr(node) {}

          Iterator operator=(const Iterator& node) {
             (*this)._curr = node._curr;
              return *this;
          }

       };

       void BuildList(void) {

           if (0 == preg.phys_next()) {
              BOOST_LOG_TRIVIAL(error) << "No List entry found!";
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
               BOOST_LOG_TRIVIAL(debug)
                   << "node entry: " << node->DebugString();
               _list.push_back(node);
               count++;
               if (!it.hasNext())
                   break;
               it = it.Next(_core);
           } while (1);
           assert (count == preg.nr_elements());
       }

       void push_back(const T& x) {

           // Allocate Node
           auto node =
               boost::shared_ptr<LinkListNode<T>> (new LinkListNode<T>(x));
           const size_t size = sizeof(node->data);
           char pbuf[size];

           auto mem = _allocator->Allocate(size);
           node->data._phys_curr = mem.first;

           // Note we update node in-place
           if (!_list.empty()) {
              auto tail = _list.rbegin();
              char qbuf[size];
              node->data._phys_prev = (*tail)->data._phys_curr;
              (*tail)->data._phys_next = node->data._phys_curr;
              (*tail)->serialize(qbuf, size);
             _core->Write((*tail)->data._phys_curr, qbuf, size);
              node->serialize(pbuf, size);
             _core->Write(node->data._phys_curr, pbuf, size);
           } else {
              node->serialize(pbuf, size);
             _core->Write(node->data._phys_curr, pbuf, size);
              preg.set_phys_next(node->data._phys_curr);
           }

           auto nr = preg.nr_elements();
           preg.set_nr_elements(nr + 1);
          _greg->update(preg);
          _list.push_back(node);

           BOOST_LOG_TRIVIAL(info) << "node pushed: " << node->DebugString();
       }

       void pop_back(void) {

           // Note preg is not updated on pop

           if (!preg.phys_next()) {
              BOOST_LOG_TRIVIAL(info) << "No List entry found!";
              return;
           }

           boost::shared_ptr<LinkListNode<T>> tail;
           for (auto iter = _list.rbegin(); iter != _list.rend(); iter++)
              if ((*iter)->data._op == LinkListNode<T>::APPEND) {
                 tail = (*iter);
                 break;
              }

           // Note we update node in-place
           if (tail) {
              const size_t size = sizeof(tail->data);
              char buf[size];
              tail->data._op = LinkListNode<T>::ERASE;
              tail->serialize(buf, size);
             _core->Write(tail->data._phys_curr, buf, size);
              BOOST_LOG_TRIVIAL(info) << "node popped: " << tail->DebugString();
           }
       }

       void clear(void) {

          if (!preg.phys_next()) {
             BOOST_LOG_TRIVIAL(info) << "No List entry found!";
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

          // Remove entry from registry
         _greg->remove(preg.key());
         _head.reset();

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

       PersistentLinkList(std::string id,
           boost::shared_ptr<Registry<IO, Allocator>> reg,
           boost::shared_ptr<IO> core, boost::shared_ptr<Allocator> alloc)
           : _core(core), _allocator(alloc), _greg(reg) {

           auto key = boost::hash_value(id);
           if (!_greg->find(key, preg))
              _greg->insert(key);

          _greg->find(key, preg);

           BOOST_LOG_TRIVIAL(info) << preg.ShortDebugString();
           if (preg.nr_elements())
              BuildList();
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
