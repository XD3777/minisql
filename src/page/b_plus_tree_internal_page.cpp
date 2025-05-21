#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);

}




/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
 int left = 1;  // 跳过第一个无效键，因为0才是开始的第一个
  int right = GetSize() - 1;
  
  while (left <= right) {
    int mid = (right + left) / 2;
    int cmp = KM.CompareKeys(key, KeyAt(mid)); 
    
    if (cmp == 0) {
      return ValueAt(mid );  // 等于时返回mid指向的子树
    } else if (cmp < 0) {
      right = mid - 1;//如果是因为这个导致不满足whlie的条件，说明在left的左边一个
    } else {
      left = mid + 1;//如果是因为这个不满足吗，说明就在right
    }
  }

  //在while外的话意味着没有恰好匹配
  
  // 返回left对应的指针
  return ValueAt(left-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetValueAt(0, old_value);  // 第一个指针指向旧的子页
  SetKeyAt(1, new_key);      // 第一个键(跳过无效的0位置)
  SetValueAt(1, new_value);  // 第二个指针指向新的子页
  SetSize(2);                // 现在有两个指针
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
   int index = ValueIndex(old_value);
  if (index == -1) return GetSize();  // 没找到，不插入
  
  // 将后面的元素向后移动
  for (int i = GetSize(); i > index + 1; --i) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  
  // 插入新的键值对
  SetKeyAt(index + 1, new_key);
  SetValueAt(index + 1, new_value);
  SetSize(GetSize()+1);
  
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int start_index = GetSize() / 2;
  int move_num = GetSize() - start_index;
  
  // 复制数据到接收页
  recipient->CopyNFrom(PairPtrAt(start_index), move_num, buffer_pool_manager);//保留了Size/2个 ，给了其他的
  
  // 调整当前页大小
  SetSize(GetSize()-move_num);

}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  int start_index = GetSize();
  
  // 复制数据
  PairCopy(PairPtrAt(start_index), src, size);
  
  // 更新子页的父指针
  for (int i = 0; i < size; i++) {
    page_id_t child_page_id = ValueAt(start_index + i);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);//待会再看，我不负责1，2现在不一定懂
  }
  
  SetSize(start_index+size);

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  if (index < 0 || index >= GetSize()) //检查index是否合理
  {
    return ;//不合理
  }
  else{
        // 2. 将后面的元素向前移动，覆盖要删除的元素
      for (int i = index; i < GetSize() - 1; ++i) {
        SetKeyAt(i, KeyAt(i + 1));
        SetValueAt(i, ValueAt(i + 1));
      }
      
      // 3. 减少大小
      SetSize(GetSize() - 1);
  }
  
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  // 1. 检查是否只有一个子节点
  if (GetSize() != 1) return INVALID_PAGE_ID;
  
  // 2. 获取并返回唯一的子节点ID
  page_id_t only_child = ValueAt(0);
  
  // 3. 重置大小
  SetSize(0);
  
  return only_child;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {//这里也默认了recipient在左边
  // 1. 将中间键插入到接收者节点
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);//因为用来merge的另一个节点指向的value没有对应的key，所以得用middle_key
  
  // 2. 复制所有键值对到接收者节点
  recipient->CopyNFrom(PairPtrAt(1), GetSize() - 1, buffer_pool_manager);
  
  // 3. 清空当前节点
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
                                      recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  
        // 2. 删除当前节点的第一个键值对
            Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int start_index = GetSize();
  
  // 复制数据

  SetKeyAt(start_index, key);
    SetValueAt(start_index, value);
  
  // 更新子页的父指针
  
    page_id_t child_page_id = ValueAt(start_index);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);//待会再看，我不负责1，2现在不一定懂

  
  SetSize(start_index+1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
            recipient->CopyFirstFrom(ValueAt(GetSize()-1), buffer_pool_manager);
            recipient->SetKeyAt(1,middle_key);
  
        // 2. 删除当前节点的最后一个键值对
            Remove(GetSize()-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int start_index = GetSize();
  

for (int i =GetSize() - 1; i >=0; i--) {//先将后面的依次后移
        SetKeyAt(i+1, KeyAt(i));
        SetValueAt(i+1, ValueAt(i));
      }


  // 复制数据
  SetValueAt(0, value);
  SetKeyAt(0, nullptr);
  
  // 更新子页的父指针
 
    page_id_t child_page_id = ValueAt(0);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);//待会再看，我不负责1，2现在不一定懂
  
  
  SetSize(start_index+1);
}