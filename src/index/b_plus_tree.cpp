#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  
 Page * page =  FindLeafPage(key, root_page_id_ , false);

  if (page == nullptr) {
        return false;
    }

 auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  
 RowId value;
  bool found =  node->Lookup(key, value, processor_);
  
  if (found) {
        // 如果找到目标键，则将关联的值添加到结果向量中
        result.push_back(value);
    }

    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);//释放叶空间

    return found;


 }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) { 
  
  if(IsEmpty())//如果是空树的话
  {
    StartNewTree(key,value);
    return true;

  }
  else{//如果不是空树的话

 bool found = InsertIntoLeaf(key, value, transaction); 
    return found;
  }
 }
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
   page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  
  // 初始化叶子节点
  auto *leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  leaf->Init(new_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  
  // 插入第一个键值对
  leaf->Insert(key, value, processor_);
  
  // 更新根页面ID
  root_page_id_ = new_page_id;
  UpdateRootPageId(true);  // true表示插入新记录
  
  // 释放页面
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) { 
  
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  // 步骤 2: 检查键是否已经存在
  RowId existing_value;
  if (leaf->Lookup(key, existing_value, processor_)) {
    // 键已经存在，返回 false
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }
  
  // 步骤 3: 插入键值对
  leaf->Insert(key, value, processor_);
  
  // 步骤 4: 检查是否需要分裂
  if (leaf->GetSize() > leaf->GetMaxSize()) {
    // 分裂叶子结点
    LeafPage *new_leaf = Split(leaf, transaction);
    
    // 获取分裂后的中间键
    GenericKey *middle_key = new_leaf->KeyAt(0);
    
    // 将中间键插入到父结点中
    InsertIntoParent(leaf, middle_key, new_leaf, transaction);
  }
  
  // 步骤 5: 释放叶子结点
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  
  return true;


}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) { 
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  
  // 初始化新中间节点
  auto *new_internal= reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  
  // 移动一半数据到新节点
  node->MoveHalfTo(new_internal);
  
  
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_internal;

}



BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) { 
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  
  // 初始化新叶子节点
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  
  // 移动一半数据到新节点
  node->MoveHalfTo(new_leaf);
  
  // 设置链表指针
  new_leaf->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_leaf->GetPageId());
  
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_leaf;

 }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  // 步骤 1: 获取父结点
  page_id_t parent_page_id = old_node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    // 如果没有父结点，创建新的根结点
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    
    // 填充新根结点
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    
    // 更新旧结点和新结点的父结点
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    
    // 更新根结点 ID
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(true);
    
    // 释放新根结点
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
  } else {
    // 如果有父结点，插入到父结点中
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    
    // 找到旧结点在父结点中的位置
    int index = parent->ValueIndex(old_node->GetPageId());
    
    // 插入新的键值对
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    
    // 检查是否需要分裂
    if (parent->GetSize() > parent->GetMaxSize()) {
      // 分裂父结点
      InternalPage *new_parent = Split(parent, transaction);
      
      // 获取分裂后的中间键
      GenericKey *middle_key = new_parent->KeyAt(0);
      
      // 递归插入到父结点的父结点中
      InsertIntoParent(parent, middle_key, new_parent, transaction);
    }
    
    // 释放父结点
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
   // 如果当前树为空，直接返回
    if (IsEmpty()) {
        return;
    }

    // 找到包含要删除键的叶子页面
    Page *leaf_page = FindLeafPage(key, root_page_id_, false);
    auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());

    // 从叶子页面中删除键值对
    int new_size = leaf->RemoveAndDeleteRecord(key, processor_);

    // 检查叶子页面是否需要合并或重新分配
    if (new_size < leaf->GetMinSize()) {
        CoalesceOrRedistribute(leaf, transaction);
    }

    // 释放叶子页面
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
   if (node->IsRootPage()) {//待会再检查检查adjustRoot
        return AdjustRoot(node);
    }

    Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    int index = parent->ValueIndex(node->GetPageId());//index是原node的键值对在parent中的位置
    int neighbor_index = (index == 0) ? 1 : index - 1;//默认sibling在左边，但当原节点index为0，sibling为右边
    page_id_t neighbor_page_id = parent->ValueAt(neighbor_index);
    Page *neighbor_page = buffer_pool_manager_->FetchPage(neighbor_page_id);
    auto *neighbor = reinterpret_cast<N *>(neighbor_page->GetData());

    if (neighbor->GetSize() + node->GetSize() <= neighbor->GetMaxSize()) {
        // 合并操作
        Coalesce(neighbor, node, parent, index, transaction);
    } else {
        // 重新分配操作
        Redistribute(neighbor, node, index);
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor->GetPageId(), true);

    return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {


    // 将节点的所有内容移动到邻居节点
    if(index !=0 )//node在sibling右边
    {
      node->MoveAllTo(neighbor_node);
      // 从父节点中删除节点
    parent->Remove(index);

      page_id_t node_page_id = node->GetPageId();
    buffer_pool_manager_->UnpinPage(node_page_id, true);  // 取消固定并标记为脏页
    buffer_pool_manager_->DeletePage(node_page_id);      // 释放物理页面


    }
    else{//node在sibling左边
      neighbor_node->MoveAllTo(node);
      // 从父节点中删除节点
    parent->Remove(index+1);

      page_id_t neighbor_node_page_id = neighbor_node->GetPageId();
    buffer_pool_manager_->UnpinPage(neighbor_node_page_id, true);  // 取消固定并标记为脏页
    buffer_pool_manager_->DeletePage(neighbor_node_page_id);      // 释放物理页面


    }


    // 检查父节点是否需要合并或重新分配
    if (parent->GetSize() < parent->GetMinSize()) {
        CoalesceOrRedistribute(parent, transaction);
    }

    return true;

}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
   // 获取分隔键
   //index是原node的键值对在parent中的位置
    GenericKey *middle_key = parent->KeyAt(index);

    // 将节点的所有内容移动到邻居节点
    if(index !=0 )//node在sibling右边
    {
      node->MoveAllTo(neighbor_node,middle_key, buffer_pool_manager_);
      // 从父节点中删除节点
    parent->Remove(index);

      page_id_t node_page_id = node->GetPageId();
    buffer_pool_manager_->UnpinPage(node_page_id, true);  // 取消固定并标记为脏页
    buffer_pool_manager_->DeletePage(node_page_id);      // 释放物理页面


    }
    else{//node在sibling左边
      GenericKey *middle_key_sibiling= parent->KeyAt(index+1);
      neighbor_node->MoveAllTo(node, middle_key_sibiling, buffer_pool_manager_);
      // 从父节点中删除节点
    parent->Remove(index+1);

      page_id_t neighbor_node_page_id = neighbor_node->GetPageId();
    buffer_pool_manager_->UnpinPage(neighbor_node_page_id, true);  // 取消固定并标记为脏页
    buffer_pool_manager_->DeletePage(neighbor_node_page_id);      // 释放物理页面


    }


    // 检查父节点是否需要合并或重新分配
    if (parent->GetSize() < parent->GetMinSize()) {
        CoalesceOrRedistribute(parent, transaction);
    }

    return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {//这里没更新parent的key啊？


  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (index == 0) {//原node在左边
        // 从邻居节点借一个元素到节点
        
        neighbor_node->MoveFirstToEndOf(node);
        parent->SetKeyAt(index+1,neighbor_node->KeyAt(0));
    } else {//原node在右边
        // 从邻居节点借一个元素到节点
        
        neighbor_node->MoveLastToFrontOf(node);
        parent->SetKeyAt(index,node->KeyAt(0));
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);

}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {

  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (index == 0) {//原node在左边
        // 从邻居节点借一个元素到节点
        Page * leftmost = FindLeafPage(nullptr, neighbor_node->GetPageId(), true);
        auto *leaf = reinterpret_cast<LeafPage *>( leftmost->GetData());
        GenericKey *middle_key = leaf->KeyAt(0);//邻居第一个子树最小值
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);

        GenericKey *tempKey = neighbor_node->KeyAt(1);


        neighbor_node->MoveFirstToEndOf(node,middle_key,buffer_pool_manager_);
        parent->SetKeyAt(index+1,tempKey);

    } else {//原node在右边
        // 从邻居节点借一个元素到节点
        Page * leftmost = FindLeafPage(nullptr, node->GetPageId(), true);
        auto *leaf = reinterpret_cast<LeafPage *>( leftmost->GetData());
        GenericKey *middle_key  = leaf->KeyAt(0);//邻居第一个子树最小值
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);


        GenericKey *tempKey = neighbor_node->KeyAt(neighbor_node->GetSize()-1);


        neighbor_node->MoveLastToFrontOf(node,middle_key,buffer_pool_manager_);
        parent->SetKeyAt(index,tempKey);
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);

}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (node->IsRootPage()) {
        if (node->IsLeafPage()) {
            if (node->GetSize() == 0) {
                // 根节点为空，树为空
                root_page_id_ = INVALID_PAGE_ID;
                UpdateRootPageId(false);
                return true;
            }
        } else {
            auto *internal = reinterpret_cast<InternalPage *>(node);
            if (internal->GetSize() == 1) {
                // 根节点只有一个子节点，将子节点提升为新的根节点
                page_id_t child_page_id = internal->RemoveAndReturnOnlyChild();
                Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
                auto *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
                child->SetParentPageId(INVALID_PAGE_ID);
                root_page_id_ = child_page_id;
                UpdateRootPageId(false);
                buffer_pool_manager_->UnpinPage(child_page_id, true);
                return true;
            }
        }
    }
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  age *leaf_page = FindLeafPage(key, root_page_id_, true);
    auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    int index = leaf->KeyIndex(key, processor_);
    IndexIterator iterator(leaf->GetPageId(), buffer_pool_manager_, index);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return iterator;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   Page *leaf_page = FindLeafPage(key, root_page_id_, false);
    auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    int index = leaf->KeyIndex(key, processor_);
    IndexIterator iterator(leaf->GetPageId(), buffer_pool_manager_, index);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return iterator;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  Page *leaf_page = FindLeafPage(nullptr, root_page_id_, true);
    auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    while (leaf->GetNextPageId() != INVALID_PAGE_ID) {
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        leaf_page = buffer_pool_manager_->FetchPage(leaf->GetNextPageId());
        leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    }
    IndexIterator iterator(leaf->GetPageId(), buffer_pool_manager_, leaf->GetSize());
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return iterator;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  // 1. 获取当前页
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  
  // 2. 如果是叶子节点，直接返回
  if (node->IsLeafPage()) {
    return page;
  }
  
  // 3. 如果是内部节点，继续向下查找
  auto *internal = reinterpret_cast<InternalPage *>(node);
  page_id_t child_page_id;
  
  if (leftMost) {
    // 查找最左边的叶子节点
    child_page_id = internal->ValueAt(0);
  } else {
    // 根据键值查找合适的子节点
    child_page_id = internal->Lookup(key, processor_);
  }
  
  // 4. 递归查找子节点
  buffer_pool_manager_->UnpinPage(page_id, false);
  return FindLeafPage(key, child_page_id, leftMost);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *header_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    if (header_page == nullptr) {
        throw std::runtime_error("Failed to fetch header page");
    }
    IndexRootsPage *index_roots_page = reinterpret_cast<IndexRootsPage *>(header_page->GetData());

    // 2. 判断操作类型
    if (insert_record) {
        // 插入操作
        index_roots_page->Insert(index_id_, root_page_id_);
    } else {
        // 更新操作
        index_roots_page->Update(index_id_, root_page_id_);
    }

    // 3. 标记页面为脏页并释放
    header_page->SetDirty(true);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}