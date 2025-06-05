# MiniSQL


本框架参考CMU-15445 BusTub框架进行改写，在保留了缓冲池、索引、记录模块的一些核心设计理念的基础上，做了一些修改和扩展，使之兼容于原MiniSQL实验指导的要求。
以下列出了改动/扩展较大的几个地方：
- 对Disk Manager模块进行改动，扩展了位图页、磁盘文件元数据页用于支持持久化数据页分配回收状态；
- 在Record Manager, Index Manager, Catalog Manager等模块中，通过对内存对象的序列化和反序列化来持久化数据；
- 对Record Manager层的一些数据结构（`Row`、`Field`、`Schema`、`Column`等）和实现进行重构；
- 对Catalog Manager层进行重构，支持持久化并为Executor层提供接口调用;
- 扩展了Parser层，Parser层支持输出语法树供Executor层调用；

此外还涉及到很多零碎的改动，包括源代码中部分模块代码的调整，测试代码的修改，性能上的优化等，在此不做赘述。


注意：为了避免代码抄袭，请不要将自己的代码发布到任何公共平台中。

### 编译&开发环境
- Apple clang version: 11.0+ (MacOS)，使用`gcc --version`和`g++ --version`查看
- gcc & g++ : 8.0+ (Linux)，使用`gcc --version`和`g++ --version`查看
- cmake: 3.20+ (Both)，使用`cmake --version`查看
- gdb: 7.0+ (Optional)，使用`gdb --version`查看
- flex & bison (暂时不需要安装，但如果需要对SQL编译器的语法进行修改，需要安装）
- llvm-symbolizer (暂时不需要安装)
    - in mac os `brew install llvm`, then set path and env variables.
    - in centos `yum install devtoolset-8-libasan-devel libasan`
    - https://www.jetbrains.com/help/clion/google-sanitizers.html#AsanChapter
    - https://www.jianshu.com/p/e4cbcd764783

### 构建
#### Windows
目前该代码暂不支持在Windows平台上的编译。但在Win10及以上的系统中，可以通过安装WSL（Windows的Linux子系统）来进行
开发和构建。WSL请选择Ubuntu子系统（推荐Ubuntu20及以上）。如果你使用Clion作为IDE，可以在Clion中配置WSL从而进行调试，具体请参考
[Clion with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends/)

#### MacOS & Linux & WSL
基本构建命令
```bash
mkdir build
cd build
cmake ..
make -j
```
若不涉及到`CMakeLists`相关文件的变动且没有新增或删除`.cpp`代码（通俗来说，就是只是对现有代码做了修改）
则无需重新执行`cmake..`命令，直接执行`make -j`编译即可。

默认以`debug`模式进行编译，如果你需要使用`release`模式进行编译：
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### 测试
在构建后，默认会在`build/test`目录下生成`minisql_test`的可执行文件，通过`./minisql_test`即可运行所有测试。

如果需要运行单个测试，例如，想要运行`lru_replacer_test.cpp`对应的测试文件，可以通过`make lru_replacer_test`
命令进行构建。

./test/disk_manager_test
./test/buffer_pool_manager_test
./test/lru_replacer_test
./test/tuple_test
./test/table_heap_test
./test/b_plus_tree_index_test
./test/b_plus_tree_test
./test/index_iterator_test
./test/catalog_test
./test/executor_test

execfile "1.txt";


create database db0;
create database db1;
show databases;
drop database db0;
show databases;

use db1;
show tables;
create table t1(a int, b char(20) unique, c float, primary key(a, c));
create table t2(a int, b char(0) unique, c float, primary key(a, c));
create table student(sno char(8),sage int,sab float unique,primary key (sno, sab));
show tables;
drop table student;
show tables;

create index idx1 on t1(a, b);
show indexes;
drop index idx1;
show indexes;

insert into t1 values(1, "aaa", null, 2.33);
insert into t1 values(3, "aaa", null);
insert into t1 values(2, "aaa", 1.111);
insert into t1 values(2, "cccc", 2.333);
insert into t1 values(1, "ddd", 1.234);
insert into t1 values(1, "eee", 1.234);

select * from t1;
select b from t1;
select * from t1 where a<2 and b = "ddd" or c is null;

delete from t1 where a = 1 and b="ddd";
select * from t1;

update t1 set a = 5, b = "ccc" where b = "aaa";
select * from t1;
```
minisql
├─ .clang-format
├─ clean.sh
├─ CMakeLists.txt
├─ LICENSE
├─ README.md
├─ reformat.sh
├─ src
│  ├─ buffer
│  │  ├─ buffer_pool_manager.cpp
│  │  └─ lru_replacer.cpp
│  ├─ catalog
│  │  ├─ catalog.cpp
│  │  ├─ indexes.cpp
│  │  └─ table.cpp
│  ├─ CMakeLists.txt
│  ├─ common
│  │  └─ instance.cpp
│  ├─ concurrency
│  │  ├─ lock_manager.cpp
│  │  └─ txn_manager.cpp
│  ├─ executor
│  │  ├─ delete_executor.cpp
│  │  ├─ execute_engine.cpp
│  │  ├─ index_scan_executor.cpp
│  │  ├─ insert_executor.cpp
│  │  ├─ seq_scan_executor.cpp
│  │  ├─ update_executor.cpp
│  │  └─ values_executor.cpp
│  ├─ include
│  │  ├─ buffer
│  │  │  ├─ buffer_pool_manager.h
│  │  │  ├─ clock_replacer.h
│  │  │  ├─ lru_replacer.h
│  │  │  └─ replacer.h
│  │  ├─ catalog
│  │  │  ├─ catalog.h
│  │  │  ├─ indexes.h
│  │  │  └─ table.h
│  │  ├─ common
│  │  │  ├─ config.h
│  │  │  ├─ dberr.h
│  │  │  ├─ instance.h
│  │  │  ├─ macros.h
│  │  │  ├─ result_writer.h
│  │  │  ├─ rowid.h
│  │  │  ├─ rwlatch.h
│  │  │  └─ singleton.h
│  │  ├─ concurrency
│  │  │  ├─ lock_manager.h
│  │  │  ├─ txn.h
│  │  │  └─ txn_manager.h
│  │  ├─ executor
│  │  │  ├─ execute_context.h
│  │  │  ├─ execute_engine.h
│  │  │  ├─ executors
│  │  │  │  ├─ abstract_executor.h
│  │  │  │  ├─ delete_executor.h
│  │  │  │  ├─ index_scan_executor.h
│  │  │  │  ├─ insert_executor.h
│  │  │  │  ├─ seq_scan_executor.h
│  │  │  │  ├─ update_executor.h
│  │  │  │  └─ values_executor.h
│  │  │  └─ plans
│  │  │     ├─ abstract_plan.h
│  │  │     ├─ delete_plan.h
│  │  │     ├─ index_scan_plan.h
│  │  │     ├─ insert_plan.h
│  │  │     ├─ seq_scan_plan.h
│  │  │     ├─ update_plan.h
│  │  │     └─ values_plan.h
│  │  ├─ index
│  │  │  ├─ basic_comparator.h
│  │  │  ├─ b_plus_tree.h
│  │  │  ├─ b_plus_tree_index.h
│  │  │  ├─ comparator.h
│  │  │  ├─ generic_key.h
│  │  │  ├─ index.h
│  │  │  └─ index_iterator.h
│  │  ├─ page
│  │  │  ├─ bitmap_page.h
│  │  │  ├─ b_plus_tree_internal_page.h
│  │  │  ├─ b_plus_tree_leaf_page.h
│  │  │  ├─ b_plus_tree_page.h
│  │  │  ├─ disk_file_meta_page.h
│  │  │  ├─ header_page.h
│  │  │  ├─ index_roots_page.h
│  │  │  ├─ page.h
│  │  │  └─ table_page.h
│  │  ├─ parser
│  │  │  ├─ compile.sh
│  │  │  ├─ minisql.l
│  │  │  ├─ minisql.y
│  │  │  ├─ minisql_lex.h
│  │  │  ├─ minisql_yacc.h
│  │  │  ├─ parser.h
│  │  │  ├─ syntax_tree.h
│  │  │  └─ syntax_tree_printer.h
│  │  ├─ planner
│  │  │  ├─ expressions
│  │  │  │  ├─ abstract_expression.h
│  │  │  │  ├─ column_value_expression.h
│  │  │  │  ├─ comparison_expression.h
│  │  │  │  ├─ constant_value_expression.h
│  │  │  │  └─ logic_expression.h
│  │  │  ├─ planner.h
│  │  │  └─ statement
│  │  │     ├─ abstract_statement.h
│  │  │     ├─ delete_statement.h
│  │  │     ├─ insert_statement.h
│  │  │     ├─ select_statement.h
│  │  │     └─ update_statement.h
│  │  ├─ record
│  │  │  ├─ column.h
│  │  │  ├─ field.h
│  │  │  ├─ row.h
│  │  │  ├─ schema.h
│  │  │  ├─ types.h
│  │  │  └─ type_id.h
│  │  ├─ recovery
│  │  │  ├─ log_manager.h
│  │  │  ├─ log_rec.h
│  │  │  └─ recovery_manager.h
│  │  ├─ storage
│  │  │  ├─ disk_manager.h
│  │  │  ├─ table_heap.h
│  │  │  └─ table_iterator.h
│  │  └─ utils
│  │     └─ tree_file_mgr.h
│  ├─ index
│  │  ├─ b_plus_tree.cpp
│  │  ├─ b_plus_tree_index.cpp
│  │  └─ index_iterator.cpp
│  ├─ main.cpp
│  ├─ page
│  │  ├─ bitmap_page.cpp
│  │  ├─ b_plus_tree_internal_page.cpp
│  │  ├─ b_plus_tree_leaf_page.cpp
│  │  ├─ b_plus_tree_page.cpp
│  │  ├─ header_page.cpp
│  │  ├─ index_roots_page.cpp
│  │  └─ table_page.cpp
│  ├─ parser
│  │  ├─ minisql_lex.c
│  │  ├─ minisql_yacc.c
│  │  ├─ parser.c
│  │  ├─ syntax_tree.c
│  │  └─ syntax_tree_printer.cpp
│  ├─ planner
│  │  └─ planner.cpp
│  ├─ record
│  │  ├─ column.cpp
│  │  ├─ row.cpp
│  │  ├─ schema.cpp
│  │  └─ types.cpp
│  └─ storage
│     ├─ disk_manager.cpp
│     ├─ table_heap.cpp
│     └─ table_iterator.cpp
├─ test
│  ├─ buffer
│  │  ├─ buffer_pool_manager_test.cpp
│  │  └─ lru_replacer_test.cpp
│  ├─ catalog
│  │  └─ catalog_test.cpp
│  ├─ CMakeLists.txt
│  ├─ concurrency
│  │  └─ lock_manager_test.cpp
│  ├─ execution
│  │  ├─ executor_test.cpp
│  │  └─ executor_test_util.h
│  ├─ include
│  │  └─ utils
│  │     └─ utils.h
│  ├─ index
│  │  ├─ b_plus_tree_index_test.cpp
│  │  ├─ b_plus_tree_test.cpp
│  │  └─ index_iterator_test.cpp
│  ├─ main_test.cpp
│  ├─ page
│  │  └─ index_roots_page_test.cpp
│  ├─ record
│  │  └─ tuple_test.cpp
│  ├─ recovery
│  │  └─ recovery_manager_test.cpp
│  └─ storage
│     ├─ disk_manager_test.cpp
│     └─ table_heap_test.cpp
└─ thirdparty
   ├─ glog
   │  ├─ .bazelci
   │  │  └─ presubmit.yml
   │  ├─ .clang-format
   │  ├─ .clang-tidy
   │  ├─ AUTHORS
   │  ├─ bazel
   │  │  ├─ example
   │  │  │  ├─ BUILD.bazel
   │  │  │  └─ main.cc
   │  │  └─ glog.bzl
   │  ├─ BUILD.bazel
   │  ├─ ChangeLog
   │  ├─ cmake
   │  │  ├─ DetermineGflagsNamespace.cmake
   │  │  ├─ FindUnwind.cmake
   │  │  ├─ GetCacheVariables.cmake
   │  │  ├─ RunCleanerTest1.cmake
   │  │  ├─ RunCleanerTest2.cmake
   │  │  ├─ RunCleanerTest3.cmake
   │  │  ├─ TestInitPackageConfig.cmake
   │  │  └─ TestPackageConfig.cmake
   │  ├─ CMakeLists.txt
   │  ├─ CONTRIBUTORS
   │  ├─ COPYING
   │  ├─ glog-config.cmake.in
   │  ├─ glog-modules.cmake.in
   │  ├─ libglog.pc.in
   │  ├─ README.rst
   │  ├─ src
   │  │  ├─ base
   │  │  │  ├─ commandlineflags.h
   │  │  │  ├─ googleinit.h
   │  │  │  └─ mutex.h
   │  │  ├─ cleanup_immediately_unittest.cc
   │  │  ├─ cleanup_with_absolute_prefix_unittest.cc
   │  │  ├─ cleanup_with_relative_prefix_unittest.cc
   │  │  ├─ config.h.cmake.in
   │  │  ├─ demangle.cc
   │  │  ├─ demangle.h
   │  │  ├─ demangle_unittest.cc
   │  │  ├─ demangle_unittest.sh
   │  │  ├─ demangle_unittest.txt
   │  │  ├─ glog
   │  │  │  ├─ logging.h.in
   │  │  │  ├─ log_severity.h
   │  │  │  ├─ platform.h
   │  │  │  ├─ raw_logging.h.in
   │  │  │  ├─ stl_logging.h.in
   │  │  │  └─ vlog_is_on.h.in
   │  │  ├─ googletest.h
   │  │  ├─ logging.cc
   │  │  ├─ logging_custom_prefix_unittest.cc
   │  │  ├─ logging_custom_prefix_unittest.err
   │  │  ├─ logging_striplog_test.sh
   │  │  ├─ logging_striptest10.cc
   │  │  ├─ logging_striptest2.cc
   │  │  ├─ logging_striptest_main.cc
   │  │  ├─ logging_unittest.cc
   │  │  ├─ logging_unittest.err
   │  │  ├─ mock-log.h
   │  │  ├─ mock-log_unittest.cc
   │  │  ├─ package_config_unittest
   │  │  │  └─ working_config
   │  │  │     ├─ CMakeLists.txt
   │  │  │     └─ glog_package_config.cc
   │  │  ├─ raw_logging.cc
   │  │  ├─ signalhandler.cc
   │  │  ├─ signalhandler_unittest.cc
   │  │  ├─ signalhandler_unittest.sh
   │  │  ├─ stacktrace.h
   │  │  ├─ stacktrace_generic-inl.h
   │  │  ├─ stacktrace_libunwind-inl.h
   │  │  ├─ stacktrace_powerpc-inl.h
   │  │  ├─ stacktrace_unittest.cc
   │  │  ├─ stacktrace_unwind-inl.h
   │  │  ├─ stacktrace_windows-inl.h
   │  │  ├─ stacktrace_x86-inl.h
   │  │  ├─ stl_logging_unittest.cc
   │  │  ├─ symbolize.cc
   │  │  ├─ symbolize.h
   │  │  ├─ symbolize_unittest.cc
   │  │  ├─ utilities.cc
   │  │  ├─ utilities.h
   │  │  ├─ utilities_unittest.cc
   │  │  ├─ vlog_is_on.cc
   │  │  └─ windows
   │  │     ├─ dirent.h
   │  │     ├─ port.cc
   │  │     ├─ port.h
   │  │     └─ preprocess.sh
   │  └─ WORKSPACE
   └─ googletest
      ├─ cmake
      │  ├─ Config.cmake.in
      │  ├─ gtest.pc.in
      │  ├─ gtest_main.pc.in
      │  ├─ internal_utils.cmake
      │  └─ libgtest.la.in
      ├─ CMakeLists.txt
      ├─ docs
      │  └─ README.md
      ├─ include
      │  └─ gtest
      │     ├─ gtest-death-test.h
      │     ├─ gtest-matchers.h
      │     ├─ gtest-message.h
      │     ├─ gtest-param-test.h
      │     ├─ gtest-printers.h
      │     ├─ gtest-spi.h
      │     ├─ gtest-test-part.h
      │     ├─ gtest-typed-test.h
      │     ├─ gtest.h
      │     ├─ gtest_pred_impl.h
      │     ├─ gtest_prod.h
      │     └─ internal
      │        ├─ custom
      │        │  ├─ gtest-port.h
      │        │  ├─ gtest-printers.h
      │        │  ├─ gtest.h
      │        │  └─ README.md
      │        ├─ gtest-death-test-internal.h
      │        ├─ gtest-filepath.h
      │        ├─ gtest-internal.h
      │        ├─ gtest-param-util.h
      │        ├─ gtest-port-arch.h
      │        ├─ gtest-port.h
      │        ├─ gtest-string.h
      │        └─ gtest-type-util.h
      ├─ README.md
      ├─ samples
      │  ├─ prime_tables.h
      │  ├─ sample1.cc
      │  ├─ sample1.h
      │  ├─ sample10_unittest.cc
      │  ├─ sample1_unittest.cc
      │  ├─ sample2.cc
      │  ├─ sample2.h
      │  ├─ sample2_unittest.cc
      │  ├─ sample3-inl.h
      │  ├─ sample3_unittest.cc
      │  ├─ sample4.cc
      │  ├─ sample4.h
      │  ├─ sample4_unittest.cc
      │  ├─ sample5_unittest.cc
      │  ├─ sample6_unittest.cc
      │  ├─ sample7_unittest.cc
      │  ├─ sample8_unittest.cc
      │  └─ sample9_unittest.cc
      ├─ scripts
      │  ├─ common.py
      │  ├─ fuse_gtest_files.py
      │  ├─ gen_gtest_pred_impl.py
      │  ├─ gtest-config.in
      │  ├─ README.md
      │  ├─ release_docs.py
      │  ├─ run_with_path.py
      │  ├─ test
      │  │  └─ Makefile
      │  ├─ upload.py
      │  └─ upload_gtest.py
      ├─ src
      │  ├─ gtest-all.cc
      │  ├─ gtest-death-test.cc
      │  ├─ gtest-filepath.cc
      │  ├─ gtest-internal-inl.h
      │  ├─ gtest-matchers.cc
      │  ├─ gtest-port.cc
      │  ├─ gtest-printers.cc
      │  ├─ gtest-test-part.cc
      │  ├─ gtest-typed-test.cc
      │  ├─ gtest.cc
      │  └─ gtest_main.cc
      └─ test
         ├─ BUILD.bazel
         ├─ googletest-break-on-failure-unittest.py
         ├─ googletest-break-on-failure-unittest_.cc
         ├─ googletest-catch-exceptions-test.py
         ├─ googletest-catch-exceptions-test_.cc
         ├─ googletest-color-test.py
         ├─ googletest-color-test_.cc
         ├─ googletest-death-test-test.cc
         ├─ googletest-death-test_ex_test.cc
         ├─ googletest-env-var-test.py
         ├─ googletest-env-var-test_.cc
         ├─ googletest-failfast-unittest.py
         ├─ googletest-failfast-unittest_.cc
         ├─ googletest-filepath-test.cc
         ├─ googletest-filter-unittest.py
         ├─ googletest-filter-unittest_.cc
         ├─ googletest-global-environment-unittest.py
         ├─ googletest-global-environment-unittest_.cc
         ├─ googletest-json-outfiles-test.py
         ├─ googletest-json-output-unittest.py
         ├─ googletest-list-tests-unittest.py
         ├─ googletest-list-tests-unittest_.cc
         ├─ googletest-listener-test.cc
         ├─ googletest-message-test.cc
         ├─ googletest-options-test.cc
         ├─ googletest-output-test-golden-lin.txt
         ├─ googletest-output-test.py
         ├─ googletest-output-test_.cc
         ├─ googletest-param-test-invalid-name1-test.py
         ├─ googletest-param-test-invalid-name1-test_.cc
         ├─ googletest-param-test-invalid-name2-test.py
         ├─ googletest-param-test-invalid-name2-test_.cc
         ├─ googletest-param-test-test.cc
         ├─ googletest-param-test-test.h
         ├─ googletest-param-test2-test.cc
         ├─ googletest-port-test.cc
         ├─ googletest-printers-test.cc
         ├─ googletest-setuptestsuite-test.py
         ├─ googletest-setuptestsuite-test_.cc
         ├─ googletest-shuffle-test.py
         ├─ googletest-shuffle-test_.cc
         ├─ googletest-test-part-test.cc
         ├─ googletest-throw-on-failure-test.py
         ├─ googletest-throw-on-failure-test_.cc
         ├─ googletest-uninitialized-test.py
         ├─ googletest-uninitialized-test_.cc
         ├─ gtest-typed-test2_test.cc
         ├─ gtest-typed-test_test.cc
         ├─ gtest-typed-test_test.h
         ├─ gtest-unittest-api_test.cc
         ├─ gtest_all_test.cc
         ├─ gtest_assert_by_exception_test.cc
         ├─ gtest_environment_test.cc
         ├─ gtest_help_test.py
         ├─ gtest_help_test_.cc
         ├─ gtest_json_test_utils.py
         ├─ gtest_list_output_unittest.py
         ├─ gtest_list_output_unittest_.cc
         ├─ gtest_main_unittest.cc
         ├─ gtest_no_test_unittest.cc
         ├─ gtest_pred_impl_unittest.cc
         ├─ gtest_premature_exit_test.cc
         ├─ gtest_prod_test.cc
         ├─ gtest_repeat_test.cc
         ├─ gtest_skip_check_output_test.py
         ├─ gtest_skip_environment_check_output_test.py
         ├─ gtest_skip_in_environment_setup_test.cc
         ├─ gtest_skip_test.cc
         ├─ gtest_sole_header_test.cc
         ├─ gtest_stress_test.cc
         ├─ gtest_testbridge_test.py
         ├─ gtest_testbridge_test_.cc
         ├─ gtest_test_macro_stack_footprint_test.cc
         ├─ gtest_test_utils.py
         ├─ gtest_throw_on_failure_ex_test.cc
         ├─ gtest_unittest.cc
         ├─ gtest_xml_outfile1_test_.cc
         ├─ gtest_xml_outfile2_test_.cc
         ├─ gtest_xml_outfiles_test.py
         ├─ gtest_xml_output_unittest.py
         ├─ gtest_xml_output_unittest_.cc
         ├─ gtest_xml_test_utils.py
         ├─ production.cc
         └─ production.h

```