CREATE TABLE `reply_0` (
  `id` bigint(11) UNSIGNED NOT NULL COMMENT '评论ID',
  `oid` int(11) NOT NULL COMMENT '业务ID',
  `type` tinyint NOT NULL COMMENT '业务类型',
  `mid` bigint(11) NOT NULL COMMENT '用户ID',
  `root` bigint(11) NOT NULL COMMENT '根评论ID',
  `parent` bigint(11) NOT NULL COMMENT '父评论ID',
  `dialog` bigint(11) NOT NULL COMMENT '对话ID',
  `count` int(11) NOT NULL COMMENT '子评论总数量',
  `rcount` int(11) NOT NULL COMMENT '可显示的子评论数量',
  `like` int(11) NOT NULL COMMENT '点赞数量',
  `hate` int(11) NOT NULL COMMENT '点踩数量',
  `floor` bigint(11) NOT NULL COMMENT '层数',
  `state` tinyint NOT NULL COMMENT '状态',
  `attr` int(11) NOT NULL COMMENT '标签',
  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `ix_mtime` (`mtime`)
) COMMENT='评论表';

CREATE TABLE `reply_subject_0` (
  `oid` int(11) NOT NULL COMMENT '业务ID',
  `type` tinyint NOT NULL COMMENT '业务类型',
  `mid` bigint(11) NOT NULL COMMENT '用户ID',
  `count` int(11) NOT NULL COMMENT '评论总数量',
  `rcount` int(11) NOT NULL COMMENT '根评论数量',
  `acount` int(11) NOT NULL COMMENT '可显示评论总数量',
  `state` tinyint NOT NULL COMMENT '状态',
  `attr` int(11) NOT NULL COMMENT '标签',
  `meta` varchar(256) NOT NULL COMMENT '元信息',
  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`oid`,`type`),
  KEY `ix_mtime` (`mtime`)
) COMMENT='评论信息表';

CREATE TABLE `reply_content_0` (
  `rpid` bigint(11) UNSIGNED NOT NULL COMMENT '评论ID',
  `message` varchar(4000) NOT NULL COMMENT '评论内容',
  `ats` blob NOT NULL COMMENT '@id',
  `ip` char(15) NOT NULL COMMENT 'ip',
  `plat` tinyint NOT NULL COMMENT '终端',
  `device` varchar(50) NOT NULL COMMENT '设备',
  `version` varchar(50) NOT NULL COMMENT '版本',

  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`rpid`),
  KEY `ix_mtime` (`mtime`)
) COMMENT='评论内容表';