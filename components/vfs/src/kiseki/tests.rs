// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{path::Path, time::SystemTime};

use fuser::TimeOrNow;
use kiseki_common::{FH, MAX_NAME_LENGTH};
use kiseki_types::{ToErrno, attr::SetAttrFlags, entry::FullEntry, ino::CONFIG_INODE};
use proptest::prelude::*;
use rstest::rstest;
use serial_test::serial;

use super::*;

/// 测试工具模块 - 提供通用的测试辅助函数和fixtures
mod test_utils {
    use rstest::fixture;

    use super::*;

    /// VFS测试环境，包含VFS实例和临时目录
    pub struct VfsTestEnv {
        pub vfs:      Arc<KisekiVFS>,
        pub ctx:      Arc<FuseContext>,
        pub _tempdir: tempfile::TempDir, // 保持临时目录不被删除
    }

    /// 创建用于测试的VFS实例 - 传统方式（向后兼容）
    pub async fn make_vfs() -> KisekiVFS {
        let tempdir = tempfile::tempdir().unwrap();
        let mut meta_config = kiseki_meta::MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", tempdir.path().to_str().unwrap()));
        let mut format = kiseki_types::setting::Format::default();
        format.with_name("test-kiseki");
        kiseki_meta::update_format(&meta_config.dsn, format, true).unwrap();

        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let vfs_config = Config {
            stage_cache_dir: tempdir.path().join("stage"),
            ..Config::default()
        };
        KisekiVFS::new(vfs_config, meta_engine).unwrap()
    }

    /// 创建用于测试的VFS实例 - rstest fixture
    #[fixture]
    pub async fn vfs_env() -> VfsTestEnv {
        let tempdir = tempfile::tempdir().unwrap();
        let mut meta_config = kiseki_meta::MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", tempdir.path().to_str().unwrap()));
        let mut format = kiseki_types::setting::Format::default();
        format.with_name("test-kiseki");
        kiseki_meta::update_format(&meta_config.dsn, format, true).unwrap();

        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let vfs_config = Config {
            stage_cache_dir: tempdir.path().join("stage"),
            ..Config::default()
        };
        let vfs = Arc::new(KisekiVFS::new(vfs_config, meta_engine).unwrap());
        let ctx = Arc::new(FuseContext::background());

        // 初始化VFS
        vfs.init(&ctx).await.unwrap();

        VfsTestEnv {
            vfs,
            ctx,
            _tempdir: tempdir,
        }
    }

    /// 创建测试文件内容生成器
    pub fn generate_test_data(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    /// 生成随机文件名
    #[allow(dead_code)]
    pub fn generate_filename(prefix: &str) -> String {
        use std::time::SystemTime;
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}_file_{}", prefix, timestamp % 10000)
    }

    /// 创建测试文件的辅助函数
    pub async fn create_test_file(
        vfs: &KisekiVFS,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
    ) -> Result<(FullEntry, FH)> {
        vfs.create(ctx, parent, name, mode, 0, libc::O_RDWR).await
    }

    /// 验证文件属性的辅助函数
    #[allow(dead_code)]
    pub fn assert_file_attr(attr: &InodeAttr, expected_mode: u32, expected_size: u64) {
        assert!(attr.is_file(), "Expected file type");
        assert_eq!(attr.mode, expected_mode, "Mode mismatch");
        assert_eq!(attr.length, expected_size, "Size mismatch");
    }

    /// 验证目录属性的辅助函数
    pub fn assert_dir_attr(attr: &InodeAttr, expected_mode: u32) {
        assert!(attr.is_dir(), "Expected directory type");
        assert_eq!(attr.mode, expected_mode, "Mode mismatch");
    }
}

/// 基础功能测试模块
mod basic_operations {
    use super::{test_utils::vfs_env, *};

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vfs_initialization(#[future] vfs_env: test_utils::VfsTestEnv) -> Result<()> {
        let env = vfs_env.await;

        // Verify filesystem is operational after init
        let root_attr = env.vfs.get_attr(ROOT_INO).await?;
        test_utils::assert_dir_attr(&root_attr, 511);

        // Verify filesystem stats are accessible
        let fs_stat = env.vfs.stat_fs(env.ctx.clone(), ROOT_INO)?;
        assert!(fs_stat.total_size > 0, "文件系统总大小应大于0");

        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_filesystem_stats(#[future] vfs_env: test_utils::VfsTestEnv) -> Result<()> {
        let env = vfs_env.await;
        let stat = env.vfs.stat_fs(env.ctx.clone(), ROOT_INO)?;

        // 验证基本统计信息
        assert!(stat.total_size > 0, "总大小应大于0");
        assert!(stat.used_size <= stat.total_size, "已用大小不应超过总大小");
        // 验证文件数是有效的（u64不能为负）
        assert!(stat.file_count <= 1_000_000, "文件数应在合理范围内");

        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_root_directory_operations(
        #[future] vfs_env: test_utils::VfsTestEnv,
    ) -> Result<()> {
        let env = vfs_env.await;

        // 测试根目录属性获取
        let root_attr = env.vfs.get_attr(ROOT_INO).await?;
        test_utils::assert_dir_attr(&root_attr, 511);

        // 测试lookup不存在的文件（应该失败）
        // 注意："." 和 ".." 是有效的目录项，所以我们测试一个不存在的文件名
        let lookup_result = env
            .vfs
            .lookup(env.ctx.clone(), ROOT_INO, "nonexistent_file")
            .await;
        assert!(lookup_result.is_err(), "查找不存在的文件应该失败");

        Ok(())
    }
}

/// 文件操作测试模块
mod file_operations {
    use super::{test_utils::vfs_env, *};

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_basic_file_io(#[future] vfs_env: test_utils::VfsTestEnv) -> Result<()> {
        let env = vfs_env.await;

        // 创建文件
        let (entry, fh) =
            test_utils::create_test_file(&env.vfs, env.ctx.clone(), ROOT_INO, "test_file", 0o755)
                .await?;

        // 写入数据
        let test_data = b"hello world";
        let write_len = env
            .vfs
            .write(env.ctx.clone(), entry.inode, fh, 0, test_data, 0, 0, None)
            .await?;
        assert_eq!(write_len, test_data.len() as u32, "写入长度应匹配");

        // 同步数据
        env.vfs
            .fsync(env.ctx.clone(), entry.inode, fh, true)
            .await?;

        // 读取数据
        let read_content = env
            .vfs
            .read(
                env.ctx.clone(),
                entry.inode,
                fh,
                0,
                test_data.len() as u32,
                0,
                None,
            )
            .await?;
        assert_eq!(read_content.as_ref(), test_data, "读取内容应匹配写入内容");

        // 清理资源
        env.vfs.release(env.ctx, entry.inode, fh).await?;

        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_returns_only_bytes_before_eof(
        #[future] vfs_env: test_utils::VfsTestEnv,
    ) -> Result<()> {
        let env = vfs_env.await;
        let (entry, fh) = test_utils::create_test_file(
            &env.vfs,
            env.ctx.clone(),
            ROOT_INO,
            "partial_eof_read",
            0o644,
        )
        .await?;
        let contents = b"hello";
        env.vfs
            .write(env.ctx.clone(), entry.inode, fh, 0, contents, 0, 0, None)
            .await?;

        let crossing_eof = env
            .vfs
            .read(env.ctx.clone(), entry.inode, fh, 0, 16, 0, None)
            .await?;
        assert_eq!(crossing_eof.as_ref(), contents);

        let at_eof = env
            .vfs
            .read(
                env.ctx.clone(),
                entry.inode,
                fh,
                contents.len() as i64,
                16,
                0,
                None,
            )
            .await?;
        assert!(at_eof.is_empty());

        env.vfs.release(env.ctx, entry.inode, fh).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_large_file_write() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let (entry, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "large_file", 0o644).await?;

        // 测试大文件写入 - 在100MB偏移处写入数据
        let large_offset = 100 << 20; // 100 MB
        let test_data = b"large_file_content";

        let write_len = vfs
            .write(
                ctx.clone(),
                entry.inode,
                fh,
                large_offset,
                test_data,
                0,
                0,
                None,
            )
            .await?;
        assert_eq!(write_len, test_data.len() as u32);

        // 同步并读取
        vfs.fsync(ctx.clone(), entry.inode, fh, true).await?;
        let read_content = vfs
            .read(
                ctx.clone(),
                entry.inode,
                fh,
                large_offset,
                test_data.len() as u32,
                0,
                None,
            )
            .await?;
        assert_eq!(read_content.as_ref(), test_data);

        // 验证writer引用计数
        let fw = vfs
            .data_manager
            .find_file_writer(entry.inode)
            .ok_or_else(|| {
                crate::err::LibcSnafu {
                    errno: libc::ENOENT,
                }
                .build()
            })?;
        assert_eq!(fw.get_reference_count(), 1, "Writer引用计数应为1");

        // 清理
        vfs.release(ctx, entry.inode, fh).await?;

        // 验证writer已清理 - 由于writer清理可能是异步的，我们给它一些时间
        // 或者检查引用计数是否降为0
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // 检查writer是否仍然存在，如果存在，引用计数应该为0
        if let Some(fw) = vfs.data_manager.find_file_writer(entry.inode) {
            // 如果writer仍然存在，那么引用计数应该为0，表示没有活跃的引用
            assert_eq!(
                fw.get_reference_count(),
                0,
                "Writer引用计数应为0表示无活跃引用"
            );
        }
        // 否则writer已被完全清理，这也是可接受的

        Ok(())
    }

    #[rstest]
    #[case(1024, "小文件_1KB")]
    #[case(4096, "页面大小_4KB")]
    #[case(65536, "大文件_64KB")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_various_file_sizes(
        #[future] vfs_env: test_utils::VfsTestEnv,
        #[case] size: usize,
        #[case] _desc: &str,
    ) -> Result<()> {
        let env = vfs_env.await;
        let filename = format!("file_{size}_bytes");
        let (entry, fh) =
            test_utils::create_test_file(&env.vfs, env.ctx.clone(), ROOT_INO, &filename, 0o644)
                .await?;

        // 生成测试数据
        let test_data = test_utils::generate_test_data(size);

        // 写入数据
        let write_len = env
            .vfs
            .write(env.ctx.clone(), entry.inode, fh, 0, &test_data, 0, 0, None)
            .await?;
        assert_eq!(write_len, size as u32);

        // 读取并验证
        let read_content = env
            .vfs
            .read(env.ctx.clone(), entry.inode, fh, 0, size as u32, 0, None)
            .await?;
        assert_eq!(read_content.as_ref(), test_data.as_slice());

        // 清理
        env.vfs.release(env.ctx, entry.inode, fh).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_file_flush_and_sync() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let (entry, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "sync_test_file", 0o644)
                .await?;

        // 写入数据
        let test_data = b"sync test data";
        vfs.write(ctx.clone(), entry.inode, fh, 0, test_data, 0, 0, None)
            .await?;

        // 测试flush操作
        vfs.flush(ctx.clone(), entry.inode, fh, 0).await?;

        // 测试fsync操作
        vfs.fsync(ctx.clone(), entry.inode, fh, false).await?;
        vfs.fsync(ctx.clone(), entry.inode, fh, true).await?;

        // 清理
        vfs.release(ctx, entry.inode, fh).await?;

        Ok(())
    }
}

/// 目录操作测试模块
mod directory_operations {
    use super::{test_utils::vfs_env, *};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_directory_creation_and_removal() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建目录
        let dir1 = vfs
            .mkdir(ctx.clone(), ROOT_INO, "test_dir1", 0o755, 0)
            .await?;
        test_utils::assert_dir_attr(&dir1.attr, 493); // 0o755 & !S_IFMT + S_IFDIR

        // 创建嵌套目录
        let dir2 = vfs
            .mkdir(ctx.clone(), dir1.inode, "test_dir2", 0o755, 0)
            .await?;
        test_utils::assert_dir_attr(&dir2.attr, 493);

        // 测试删除非空目录（应该失败）
        let rmdir_result = vfs.rmdir(ctx.clone(), ROOT_INO, &dir1.name).await;
        assert!(rmdir_result.is_err(), "删除非空目录应该失败");
        assert_eq!(rmdir_result.unwrap_err().to_errno(), libc::ENOTEMPTY);

        // 先删除子目录，再删除父目录
        vfs.rmdir(ctx.clone(), dir1.inode, &dir2.name).await?;
        vfs.rmdir(ctx.clone(), ROOT_INO, &dir1.name).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_directory_listing() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建多个目录和文件
        let _dir1 = vfs
            .mkdir(ctx.clone(), ROOT_INO, "list_test_dir1", 0o755, 0)
            .await?;
        let _dir2 = vfs
            .mkdir(ctx.clone(), ROOT_INO, "list_test_dir2", 0o755, 0)
            .await?;

        let (file1, fh1) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "list_test_file1", 0o644)
                .await?;
        vfs.release(ctx.clone(), file1.inode, fh1).await?;

        // 打开根目录并读取条目
        let root_handle = vfs.open_dir(&ctx, ROOT_INO, libc::O_RDONLY).await?;
        let entries = vfs.read_dir(&ctx, ROOT_INO, root_handle, 0, true).await?;

        // 验证条目数量（应该有3个：两个目录 + 一个文件）
        assert_eq!(entries.len(), 3, "应该有3个条目");

        // 验证条目名称
        let entry_names: std::collections::HashSet<String> =
            entries.iter().map(|e| e.get_name().to_string()).collect();
        assert!(entry_names.contains("list_test_dir1"));
        assert!(entry_names.contains("list_test_dir2"));
        assert!(entry_names.contains("list_test_file1"));

        // 释放目录句柄
        vfs.release_dir(ROOT_INO, root_handle).await?;

        Ok(())
    }

    #[rstest]
    #[case("简单目录", "中文目录名")]
    #[case("带_下划线_的_目录", "包含下划线的目录名")]
    #[case("Dir123", "英文数字混合目录名")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_directory_names(
        #[future] vfs_env: test_utils::VfsTestEnv,
        #[case] dirname: &str,
        #[case] _desc: &str,
    ) -> Result<()> {
        let env = vfs_env.await;

        // 创建目录
        let dir = env
            .vfs
            .mkdir(env.ctx.clone(), ROOT_INO, dirname, 0o755, 0)
            .await?;
        assert_eq!(dir.name, dirname, "目录名应匹配");

        // 通过lookup验证目录存在
        let lookup_result = env.vfs.lookup(env.ctx.clone(), ROOT_INO, dirname).await?;
        assert_eq!(lookup_result.inode, dir.inode, "Lookup结果应匹配");

        // 清理
        env.vfs.rmdir(env.ctx.clone(), ROOT_INO, dirname).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_nested_directory_operations() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建深层嵌套目录结构 a/b/c/d
        let dir_a = vfs.mkdir(ctx.clone(), ROOT_INO, "a", 0o755, 0).await?;
        let dir_b = vfs.mkdir(ctx.clone(), dir_a.inode, "b", 0o755, 0).await?;
        let dir_c = vfs.mkdir(ctx.clone(), dir_b.inode, "c", 0o755, 0).await?;
        let dir_d = vfs.mkdir(ctx.clone(), dir_c.inode, "d", 0o755, 0).await?;

        // 在最深层目录中创建文件
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), dir_d.inode, "deep_file", 0o644)
                .await?;

        // 写入并读取数据验证深层文件操作
        let test_data = b"deep file content";
        vfs.write(ctx.clone(), file.inode, fh, 0, test_data, 0, 0, None)
            .await?;
        let read_data = vfs
            .read(
                ctx.clone(),
                file.inode,
                fh,
                0,
                test_data.len() as u32,
                0,
                None,
            )
            .await?;
        assert_eq!(read_data.as_ref(), test_data);

        vfs.release(ctx.clone(), file.inode, fh).await?;

        // 先删除文件，再删除目录（目录必须为空才能删除）
        vfs.unlink(ctx.clone(), dir_d.inode, &file.name).await?;

        // 自底向上删除目录结构
        vfs.rmdir(ctx.clone(), dir_c.inode, &dir_d.name).await?;
        vfs.rmdir(ctx.clone(), dir_b.inode, &dir_c.name).await?;
        vfs.rmdir(ctx.clone(), dir_a.inode, &dir_b.name).await?;
        vfs.rmdir(ctx.clone(), ROOT_INO, &dir_a.name).await?;

        Ok(())
    }
}

/// 链接操作测试模块
mod link_operations {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_hard_link_operations() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建原始文件
        let dir1 = vfs
            .mkdir(ctx.clone(), ROOT_INO, "link_test_dir", 0o755, 0)
            .await?;
        let (original_file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), dir1.inode, "original_file", 0o644)
                .await?;

        // 写入数据
        let test_data = b"original content";
        vfs.write(
            ctx.clone(),
            original_file.inode,
            fh,
            0,
            test_data,
            0,
            0,
            None,
        )
        .await?;
        vfs.release(ctx.clone(), original_file.inode, fh).await?;

        // 创建硬链接
        let hard_link = vfs
            .link(ctx.clone(), original_file.inode, ROOT_INO, "hard_link")
            .await?;

        // 验证链接计数
        let file_attr = vfs.get_attr(original_file.inode).await?;
        assert_eq!(file_attr.nlink, 2, "硬链接计数应为2");

        // 通过硬链接读取数据验证内容一致性
        let link_opened = vfs.open(&ctx, hard_link.inode, libc::O_RDONLY).await?;
        let read_data = vfs
            .read(
                ctx.clone(),
                link_opened.inode,
                link_opened.fh,
                0,
                test_data.len() as u32,
                0,
                None,
            )
            .await?;
        assert_eq!(read_data.as_ref(), test_data, "通过硬链接读取的数据应一致");
        vfs.release(ctx.clone(), link_opened.inode, link_opened.fh)
            .await?;

        // 删除原始文件，硬链接应该仍然存在
        vfs.unlink(ctx.clone(), dir1.inode, &original_file.name)
            .await?;

        // 验证硬链接仍可访问且链接计数减少
        let lookup_result = vfs.lookup(ctx.clone(), ROOT_INO, &hard_link.name).await?;
        assert_eq!(lookup_result.attr.nlink, 1, "删除原文件后链接计数应为1");

        // 清理
        vfs.unlink(ctx.clone(), ROOT_INO, &hard_link.name).await?;
        vfs.rmdir(ctx.clone(), ROOT_INO, &dir1.name).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_symbolic_link_operations() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建目标文件
        let (target_file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "symlink_target", 0o644)
                .await?;
        vfs.release(ctx.clone(), target_file.inode, fh).await?;

        // 创建符号链接
        let symlink = vfs
            .symlink(
                ctx.clone(),
                ROOT_INO,
                "test_symlink",
                Path::new("symlink_target"),
            )
            .await?;

        // 验证符号链接属性
        let symlink_attr = vfs.get_attr(symlink.inode).await?;
        assert!(
            matches!(symlink_attr.kind, fuser::FileType::Symlink),
            "应该是符号链接类型"
        );

        // 读取符号链接目标
        let link_target = vfs.readlink(ctx.clone(), symlink.inode).await?;
        assert_eq!(
            link_target.as_ref(),
            b"symlink_target",
            "符号链接目标应匹配"
        );

        // 清理
        vfs.unlink(ctx.clone(), ROOT_INO, &symlink.name).await?;
        vfs.unlink(ctx.clone(), ROOT_INO, &target_file.name).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rename_operations() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建测试文件
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "rename_source", 0o644)
                .await?;
        vfs.release(ctx.clone(), file.inode, fh).await?;

        // 重命名文件
        vfs.rename(
            ctx.clone(),
            ROOT_INO,
            &file.name,
            ROOT_INO,
            "rename_target",
            0,
        )
        .await?;

        // 验证原文件名不存在
        let old_lookup = vfs.lookup(ctx.clone(), ROOT_INO, &file.name).await;
        assert!(old_lookup.is_err(), "原文件名应不存在");
        assert_eq!(old_lookup.unwrap_err().to_errno(), libc::ENOENT);

        // 验证新文件名存在
        let new_lookup = vfs.lookup(ctx.clone(), ROOT_INO, "rename_target").await?;
        assert_eq!(new_lookup.inode, file.inode, "重命名后的inode应一致");

        // 清理
        vfs.unlink(ctx.clone(), ROOT_INO, "rename_target").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_rename_flags_return_einval() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let error = vfs
            .rename(ctx, ROOT_INO, "source", ROOT_INO, "target", u32::MAX)
            .await
            .unwrap_err();
        assert_eq!(error.to_errno(), libc::EINVAL);
        Ok(())
    }
}

/// 错误处理测试模块
mod error_handling {
    use super::{test_utils::vfs_env, *};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_file_not_found_errors() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 尝试打开不存在的文件
        let open_result = vfs.open(&ctx, Ino(999999), libc::O_RDONLY).await;
        assert!(open_result.is_err(), "打开不存在的文件应该失败");

        // 尝试lookup不存在的文件
        let lookup_result = vfs.lookup(ctx.clone(), ROOT_INO, "nonexistent_file").await;
        assert!(lookup_result.is_err(), "查找不存在的文件应该失败");
        assert_eq!(lookup_result.unwrap_err().to_errno(), libc::ENOENT);

        // 尝试删除不存在的文件
        let unlink_result = vfs.unlink(ctx.clone(), ROOT_INO, "nonexistent_file").await;
        assert!(unlink_result.is_err(), "删除不存在的文件应该失败");
        assert_eq!(unlink_result.unwrap_err().to_errno(), libc::ENOENT);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_permission_errors() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建只读文件
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "readonly_file", 0o444)
                .await?;
        vfs.release(ctx.clone(), file.inode, fh).await?;

        // 验证权限检查
        let file_attr = vfs.get_attr(file.inode).await?;
        let access_result = vfs.check_access(ctx.clone(), file.inode, &file_attr, libc::W_OK);
        assert!(access_result.is_err(), "对只读文件的写权限检查应该失败");
        assert_eq!(access_result.unwrap_err().to_errno(), libc::EACCES);

        // 测试执行权限
        let exec_result = vfs.check_access(ctx.clone(), file.inode, &file_attr, libc::X_OK);
        assert!(exec_result.is_err(), "对非可执行文件的执行权限检查应该失败");
        assert_eq!(exec_result.unwrap_err().to_errno(), libc::EACCES);

        // 清理
        vfs.unlink(ctx.clone(), ROOT_INO, &file.name).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_invalid_operations() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 尝试在文件上执行目录操作
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "regular_file", 0o644)
                .await?;
        vfs.release(ctx.clone(), file.inode, fh).await?;

        // 尝试对文件进行mkdir操作（应该失败）
        let mkdir_result = vfs.mkdir(ctx.clone(), file.inode, "subdir", 0o755, 0).await;
        assert!(mkdir_result.is_err(), "在文件中创建目录应该失败");

        // 尝试对文件进行目录列表操作
        let opendir_result = vfs.open_dir(&ctx, file.inode, libc::O_RDONLY).await;
        assert!(opendir_result.is_err(), "对文件打开目录句柄应该失败");

        // 清理
        vfs.unlink(ctx.clone(), ROOT_INO, &file.name).await?;

        Ok(())
    }

    #[rstest]
    #[case("", "空文件名")]
    #[case(".", "当前目录符号")]
    #[case("..", "父目录符号")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_invalid_file_names(
        #[future] vfs_env: test_utils::VfsTestEnv,
        #[case] invalid_name: &str,
        #[case] _desc: &str,
    ) -> Result<()> {
        let env = vfs_env.await;

        // 尝试创建具有无效名称的文件
        let create_result = env
            .vfs
            .create(
                env.ctx.clone(),
                ROOT_INO,
                invalid_name,
                0o644,
                0,
                libc::O_RDWR,
            )
            .await;
        assert!(create_result.is_err(), "创建具有无效名称的文件应该失败");

        // 尝试创建具有无效名称的目录
        let mkdir_result = env
            .vfs
            .mkdir(env.ctx.clone(), ROOT_INO, invalid_name, 0o755, 0)
            .await;
        assert!(mkdir_result.is_err(), "创建具有无效名称的目录应该失败");

        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_io_ranges_return_stable_errno(
        #[future] vfs_env: test_utils::VfsTestEnv,
    ) -> Result<()> {
        let env = vfs_env.await;
        let (entry, fh) = test_utils::create_test_file(
            &env.vfs,
            env.ctx.clone(),
            ROOT_INO,
            "invalid_io_ranges",
            0o644,
        )
        .await?;

        let read_error = env
            .vfs
            .read(env.ctx.clone(), entry.inode, fh, -1, 1, 0, None)
            .await
            .unwrap_err();
        assert_eq!(read_error.to_errno(), libc::EINVAL);

        let write_error = env
            .vfs
            .write(env.ctx.clone(), entry.inode, fh, -1, b"x", 0, 0, None)
            .await
            .unwrap_err();
        assert_eq!(write_error.to_errno(), libc::EINVAL);

        let large_offset_error = env
            .vfs
            .read(env.ctx.clone(), entry.inode, fh, i64::MAX, 1, 0, None)
            .await
            .unwrap_err();
        assert_eq!(large_offset_error.to_errno(), libc::EFBIG);

        env.vfs.release(env.ctx, entry.inode, fh).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fallocate_is_nonfatal_and_honest(
        #[future] vfs_env: test_utils::VfsTestEnv,
    ) -> Result<()> {
        let env = vfs_env.await;
        let (entry, fh) = test_utils::create_test_file(
            &env.vfs,
            env.ctx.clone(),
            ROOT_INO,
            "unsupported_fallocate",
            0o644,
        )
        .await?;

        let unsupported = env
            .vfs
            .fallocate(env.ctx.clone(), entry.inode, fh, 0, 1, 0)
            .await
            .unwrap_err();
        assert_eq!(unsupported.to_errno(), libc::ENOTSUP);

        let invalid = env
            .vfs
            .fallocate(env.ctx.clone(), entry.inode, fh, 0, 1, 0x80)
            .await
            .unwrap_err();
        assert_eq!(invalid.to_errno(), libc::EINVAL);

        let root_attr = env.vfs.get_attr(ROOT_INO).await?;
        assert!(root_attr.is_dir());
        env.vfs.release(env.ctx, entry.inode, fh).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn synthetic_inode_io_is_unsupported_and_release_is_idempotent(
        #[future] vfs_env: test_utils::VfsTestEnv,
    ) -> Result<()> {
        let env = vfs_env.await;
        let opened = env.vfs.open(&env.ctx, CONFIG_INODE, libc::O_RDONLY).await?;

        let read_error = env
            .vfs
            .read(env.ctx.clone(), CONFIG_INODE, opened.fh, 0, 16, 0, None)
            .await
            .unwrap_err();
        assert_eq!(read_error.to_errno(), libc::ENOTSUP);

        let write_error = env
            .vfs
            .write(
                env.ctx.clone(),
                CONFIG_INODE,
                opened.fh,
                0,
                b"x",
                0,
                0,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(write_error.to_errno(), libc::ENOTSUP);

        env.vfs
            .release(env.ctx.clone(), CONFIG_INODE, opened.fh)
            .await?;
        env.vfs
            .release(env.ctx.clone(), CONFIG_INODE, opened.fh)
            .await?;

        let stale_handle = env
            .vfs
            .read(env.ctx.clone(), CONFIG_INODE, opened.fh, 0, 1, 0, None)
            .await
            .unwrap_err();
        assert_eq!(stale_handle.to_errno(), libc::EBADF);
        assert!(env.vfs.get_attr(ROOT_INO).await?.is_dir());
        Ok(())
    }
}

/// 并发操作测试模块
mod concurrent_operations {
    use tokio::task::JoinSet;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_file_access() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建文件
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "concurrent_file", 0o644)
                .await?;

        // 写入初始数据
        let initial_data = b"initial content";
        vfs.write(ctx.clone(), file.inode, fh, 0, initial_data, 0, 0, None)
            .await?;
        vfs.fsync(ctx.clone(), file.inode, fh, true).await?;
        vfs.release(ctx.clone(), file.inode, fh).await?;

        // 启动多个并发读取任务
        let mut tasks: JoinSet<Result<()>> = JoinSet::new();
        for i in 0..10 {
            let vfs_clone = vfs.clone();
            let ctx_clone = ctx.clone();
            let file_inode = file.inode;

            tasks.spawn(async move {
                let opened = vfs_clone
                    .open(&ctx_clone, file_inode, libc::O_RDONLY)
                    .await?;
                let read_data = vfs_clone
                    .read(
                        ctx_clone.clone(),
                        opened.inode,
                        opened.fh,
                        0,
                        initial_data.len() as u32,
                        0,
                        None,
                    )
                    .await?;
                vfs_clone
                    .release(ctx_clone, opened.inode, opened.fh)
                    .await?;

                assert_eq!(read_data.as_ref(), initial_data, "任务{i}读取的数据应一致");
                Ok(())
            });
        }

        // 等待所有任务完成
        while let Some(result) = tasks.join_next().await {
            result.unwrap()?;
        }

        // 清理
        vfs.unlink(ctx, ROOT_INO, &file.name).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial] // 使用serial确保测试不会相互干扰
    async fn test_concurrent_directory_operations() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let mut tasks: JoinSet<Result<String>> = JoinSet::new();

        // 并发创建多个目录
        for i in 0..5 {
            let vfs_clone = vfs.clone();
            let ctx_clone = ctx.clone();

            tasks.spawn(async move {
                let dir_name = format!("concurrent_dir_{i}");
                let dir = vfs_clone
                    .mkdir(ctx_clone.clone(), ROOT_INO, &dir_name, 0o755, 0)
                    .await?;

                // 在每个目录中创建一个文件
                let (file, fh) = test_utils::create_test_file(
                    &vfs_clone,
                    ctx_clone.clone(),
                    dir.inode,
                    "file_in_dir",
                    0o644,
                )
                .await?;
                vfs_clone.release(ctx_clone, file.inode, fh).await?;

                Ok(dir_name)
            });
        }

        // 收集创建的目录名
        let mut created_dirs = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let dir_name = result.unwrap()?;
            created_dirs.push(dir_name);
        }

        // 验证所有目录都已创建
        assert_eq!(created_dirs.len(), 5, "应该创建5个目录");

        // 清理所有创建的目录
        for dir_name in &created_dirs {
            // 先删除目录中的文件
            let dir_lookup = vfs.lookup(ctx.clone(), ROOT_INO, dir_name).await?;
            vfs.unlink(ctx.clone(), dir_lookup.inode, "file_in_dir")
                .await?;
            // 再删除目录
            vfs.rmdir(ctx.clone(), ROOT_INO, dir_name).await?;
        }

        Ok(())
    }
}

/// 边界条件测试模块
mod boundary_conditions {
    use super::{test_utils::vfs_env, *};

    proptest! {
            #[test]
            fn test_filename_generation(name in "[a-zA-Z0-9_-]{1,100}") {
                // 属性测试：验证生成的文件名符合预期格式
                prop_assert!(name.len() <= 100);
                prop_assert!(name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-'));
            }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_maximum_filename_length() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 测试接近最大文件名长度的情况
        let long_name = "a".repeat(MAX_NAME_LENGTH - 1);
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, &long_name, 0o644).await?;
        vfs.release(ctx.clone(), file.inode, fh).await?;

        // 验证文件可以正常查找
        let lookup_result = vfs.lookup(ctx.clone(), ROOT_INO, &long_name).await?;
        assert_eq!(lookup_result.inode, file.inode);

        // 清理
        vfs.unlink(ctx, ROOT_INO, &long_name).await?;

        Ok(())
    }

    #[rstest]
    #[case(0, "空文件")]
    #[case(1, "单字节文件")]
    #[case(4095, "页面边界减一")]
    #[case(4097, "页面边界加一")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_edge_file_sizes(
        #[future] vfs_env: test_utils::VfsTestEnv,
        #[case] size: usize,
        #[case] _desc: &str,
    ) -> Result<()> {
        let env = vfs_env.await;
        let filename = format!("edge_file_{size}");
        let (file, fh) =
            test_utils::create_test_file(&env.vfs, env.ctx.clone(), ROOT_INO, &filename, 0o644)
                .await?;

        if size > 0 {
            let test_data = test_utils::generate_test_data(size);
            let write_len = env
                .vfs
                .write(env.ctx.clone(), file.inode, fh, 0, &test_data, 0, 0, None)
                .await?;
            assert_eq!(write_len, size as u32);

            // 读取并验证
            let read_data = env
                .vfs
                .read(env.ctx.clone(), file.inode, fh, 0, size as u32, 0, None)
                .await?;
            assert_eq!(read_data.as_ref(), test_data.as_slice());
        }

        // 验证文件属性
        let attr = env.vfs.get_attr(file.inode).await?;
        assert_eq!(attr.length, size as u64);

        // 清理
        env.vfs.release(env.ctx.clone(), file.inode, fh).await?;
        env.vfs.unlink(env.ctx, ROOT_INO, &filename).await?;

        Ok(())
    }
}

/// 属性操作测试模块
mod attribute_operations {
    use super::{test_utils::vfs_env, *};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_set_file_attributes() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        // 创建测试文件
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "attr_test_file", 0o644)
                .await?;
        vfs.release(ctx.clone(), file.inode, fh).await?;

        let test_time = SystemTime::now();

        // 设置普通用户可以修改的属性（避免UID/GID权限问题）
        // 注意：在实际系统中，修改UID/GID需要root权限，这里只测试普通属性
        let new_attr = vfs
            .set_attr(
                ctx.clone(),
                file.inode,
                (SetAttrFlags::MODE
                    | SetAttrFlags::SIZE
                    | SetAttrFlags::ATIME
                    | SetAttrFlags::MTIME)
                    .bits(),
                Some(TimeOrNow::SpecificTime(test_time)),
                Some(TimeOrNow::SpecificTime(test_time)),
                Some(0o755),
                None, // 不设置UID，避免权限问题
                None, // 不设置GID，避免权限问题
                Some(2048),
                None,
                None,
            )
            .await?;

        // 验证属性设置成功
        assert_eq!(new_attr.mode & 0o777, 0o755, "文件模式应已更新");
        assert_eq!(new_attr.length, 2048, "文件大小应已更新");
        assert_eq!(new_attr.atime, test_time, "访问时间应已更新");
        assert_eq!(new_attr.mtime, test_time, "修改时间应已更新");
        // UID/GID保持原值，因为我们没有root权限修改它们

        // 清理
        vfs.unlink(ctx, ROOT_INO, &file.name).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn truncate_open_file_updates_length_and_zero_fills_growth() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "truncate-open", 0o644)
                .await?;

        vfs.write(ctx.clone(), file.inode, fh, 8192, b"tail", 0, 0, None)
            .await?;
        for size in [3, 10] {
            let attr = vfs
                .set_attr(
                    ctx.clone(),
                    file.inode,
                    SetAttrFlags::SIZE.bits(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(size),
                    Some(fh),
                    None,
                )
                .await?;
            assert_eq!(attr.length, size);
        }

        let actual = vfs
            .read(ctx.clone(), file.inode, fh, 0, 32, 0, None)
            .await?;
        assert_eq!(actual.as_ref(), &[0; 10]);
        vfs.release(ctx, file.inode, fh).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn atime_only_setattr_does_not_require_mtime() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "atime_only", 0o644).await?;
        let atime = SystemTime::now();

        let attr = vfs
            .set_attr(
                ctx.clone(),
                file.inode,
                SetAttrFlags::ATIME.bits(),
                Some(TimeOrNow::SpecificTime(atime)),
                None,
                None,
                None,
                None,
                None,
                Some(fh),
                None,
            )
            .await?;
        assert_eq!(attr.atime, atime);

        vfs.release(ctx, file.inode, fh).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_setattr_flags_return_einval() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());
        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "bad_setattr", 0o644).await?;

        let error = vfs
            .set_attr(
                ctx.clone(),
                file.inode,
                u32::MAX,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(fh),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.to_errno(), libc::EINVAL);

        vfs.release(ctx, file.inode, fh).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_set_privileged_file_attributes() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);

        // 创建一个模拟的root context用于测试权限操作
        let mut root_ctx = FuseContext::background();
        root_ctx.uid = 0; // root用户
        root_ctx.gid = 0; // root组
        root_ctx.check_permission = false; // 跳过权限检查以简化测试
        let ctx = Arc::new(root_ctx);

        // 创建测试文件
        let (file, fh) = test_utils::create_test_file(
            &vfs,
            ctx.clone(),
            ROOT_INO,
            "privileged_attr_test_file",
            0o644,
        )
        .await?;
        vfs.release(ctx.clone(), file.inode, fh).await?;

        // 设置需要特殊权限的属性（UID/GID）
        let new_attr = vfs
            .set_attr(
                ctx.clone(),
                file.inode,
                (SetAttrFlags::UID | SetAttrFlags::GID).bits(),
                None,
                None,
                None,
                Some(1001), // 设置UID
                Some(1002), // 设置GID
                None,
                None,
                None,
            )
            .await?;

        // 验证特权属性设置成功
        assert_eq!(new_attr.uid, 1001, "用户ID应已更新");
        assert_eq!(new_attr.gid, 1002, "组ID应已更新");

        // 清理
        vfs.unlink(ctx, ROOT_INO, &file.name).await?;

        Ok(())
    }

    #[rstest]
    #[case(0o000, "无权限")]
    #[case(0o444, "只读权限")]
    #[case(0o644, "用户读写_组只读")]
    #[case(0o755, "用户全权限_组读执行")]
    #[case(0o777, "所有用户全权限")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_file_permissions(
        #[future] vfs_env: test_utils::VfsTestEnv,
        #[case] mode: u32,
        #[case] _desc: &str,
    ) -> Result<()> {
        let env = vfs_env.await;
        let filename = format!("perm_test_{mode:o}");
        let (file, fh) =
            test_utils::create_test_file(&env.vfs, env.ctx.clone(), ROOT_INO, &filename, mode)
                .await?;
        env.vfs.release(env.ctx.clone(), file.inode, fh).await?;

        // 获取并验证权限
        let attr = env.vfs.get_attr(file.inode).await?;
        // 文件创建时会应用umask，所以我们只验证用户有意设置的权限位
        // 注意：实际的mode包含文件类型位，我们只比较权限位部分(0o777)
        assert_eq!(attr.mode & 0o777, mode & 0o777, "权限位应匹配设置的权限");

        // 清理
        env.vfs.unlink(env.ctx, ROOT_INO, &filename).await?;

        Ok(())
    }
}

/// 性能测试模块
mod performance_tests {
    use std::time::Instant;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_file_creation_performance() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let start = Instant::now();
        let file_count = 100;

        // 创建多个文件并测量性能
        for i in 0..file_count {
            let filename = format!("perf_file_{i}");
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, &filename, 0o644).await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;
        }

        let elapsed = start.elapsed();
        let files_per_sec = file_count as f64 / elapsed.as_secs_f64();

        // 验证性能至少达到合理水平 (比如每秒50个文件)
        assert!(
            files_per_sec > 50.0,
            "文件创建性能太低：{files_per_sec:.2} files/sec"
        );

        println!("文件创建性能：{files_per_sec:.2} files/sec");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_directory_operations_performance() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let start = Instant::now();
        let dir_count = 50;

        // 创建多个目录
        for i in 0..dir_count {
            let dirname = format!("perf_dir_{i}");
            let _dir = vfs.mkdir(ctx.clone(), ROOT_INO, &dirname, 0o755, 0).await?;
        }

        let create_elapsed = start.elapsed();

        // 测试目录列举性能
        let list_start = Instant::now();
        let root_handle = vfs.open_dir(&ctx, ROOT_INO, libc::O_RDONLY).await?;
        let entries = vfs.read_dir(&ctx, ROOT_INO, root_handle, 0, true).await?;
        let list_elapsed = list_start.elapsed();

        vfs.release_dir(ROOT_INO, root_handle).await?;

        let dirs_per_sec = dir_count as f64 / create_elapsed.as_secs_f64();

        // 验证目录创建性能
        assert!(
            dirs_per_sec > 100.0,
            "目录创建性能太低：{dirs_per_sec:.2} dirs/sec"
        );

        // 验证目录列举性能 (应该很快)
        assert!(
            list_elapsed.as_millis() < 100,
            "目录列举太慢：{list_elapsed:?}"
        );

        // 验证列举结果正确
        assert_eq!(entries.len(), dir_count, "列举的目录数应匹配");

        println!("目录创建性能：{dirs_per_sec:.2} dirs/sec");
        println!("目录列举性能：{list_elapsed:?}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_file_io_performance() -> Result<()> {
        let vfs = Arc::new(test_utils::make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let (file, fh) =
            test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "perf_io_file", 0o644)
                .await?;

        // 测试写入性能 - 1MB数据
        let data_size = 1024 * 1024; // 1MB
        let test_data = test_utils::generate_test_data(data_size);

        let write_start = Instant::now();
        let written = vfs
            .write(ctx.clone(), file.inode, fh, 0, &test_data, 0, 0, None)
            .await?;
        let write_elapsed = write_start.elapsed();

        assert_eq!(written, data_size as u32);

        // 测试读取性能
        let read_start = Instant::now();
        let read_data = vfs
            .read(ctx.clone(), file.inode, fh, 0, data_size as u32, 0, None)
            .await?;
        let read_elapsed = read_start.elapsed();

        assert_eq!(read_data.len(), data_size);

        // 计算吞吐量 (MB/s)
        let write_throughput = (data_size as f64) / (1024.0 * 1024.0) / write_elapsed.as_secs_f64();
        let read_throughput = (data_size as f64) / (1024.0 * 1024.0) / read_elapsed.as_secs_f64();

        // 验证I/O性能至少达到1MB/s (这是很保守的要求)
        assert!(
            write_throughput > 1.0,
            "写入吞吐量太低：{write_throughput:.2} MB/s"
        );
        assert!(
            read_throughput > 1.0,
            "读取吞吐量太低：{read_throughput:.2} MB/s"
        );

        println!("写入性能：{write_throughput:.2} MB/s");
        println!("读取性能：{read_throughput:.2} MB/s");

        // 清理
        vfs.release(ctx.clone(), file.inode, fh).await?;
        vfs.unlink(ctx, ROOT_INO, &file.name).await?;

        Ok(())
    }
}
