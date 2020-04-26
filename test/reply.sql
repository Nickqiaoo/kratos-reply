create database kratos_demo;
use kratos_demo;

CREATE TABLE `reply` (
  `id` bigint(11) UNSIGNED NOT NULL COMMENT '主键ID',
  `oid` int(11) NOT NULL COMMENT '业务ID',
  `type` tinyint NOT NULL COMMENT '业务类型',
  `mid` bigint(11) NOT NULL COMMENT '用户ID',
  `root` bigint（11) NOT NULL COMMENT '根评论ID',
  `parent` bigint（11) NOT NULL COMMENT '父评论ID',
  `dialog` bigint（11) NOT NULL COMMENT '对话ID',
  `count` int(11) NOT NULL COMMENT '子评论数量',
  `rcount` int(11) NOT NULL COMMENT '子评论数量',
  `like` int(11) NOT NULL COMMENT '点赞数量',
  `floor` int(11) NOT NULL COMMENT '点踩数量',
  `state` tinyint NOT NULL COMMENT '状态',
  `attr` int(11) NOT NULL COMMENT '标签',
  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `ix_mtime` (`mtime`)
) COMMENT='文章表';