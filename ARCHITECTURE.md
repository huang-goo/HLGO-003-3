# 边缘-云协同异常推理链路 - 架构说明

## 系统架构概览

本系统实现了一个完整的边缘-云协同异常检测管道，具有端到端 exactly-once 语义保证。

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  边缘设备层      │────▶│  Greengrass      │────▶│   云端处理层     │
│  (传感器数据)    │     │  StreamManager  │     │   (Flink)       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
    数据采集              预聚合/去重                二次评分/告警
    边缘异常检测         时钟漂移校正                自适应阈值
                        断网补传                  多通道通知
```

## 核心功能模块

### 1. 数据模型层 (`edge-cloud-common`)

**核心类：**
- [AnomalyMessage.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-cloud-common/src/main/java/com/amazonaws/services/common/model/AnomalyMessage.java) - 统一消息格式
  - `message_id`: 全局唯一标识
  - `idempotency_key`: 幂等键，用于去重
  - `sequence_number`: 序列号，用于顺序检测
  - `edge_timestamp_ms`: 边缘时间戳
  - `corrected_timestamp_ms`: 校正后的时间戳
  - `clock_offset_ms`: 时钟偏移量
  - `anomaly_scores`: 各分组异常分数
  - `processing_stage`: 处理阶段标记

- [AggregatedWindow.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-cloud-common/src/main/java/com/amazonaws/services/common/model/AggregatedWindow.java) - 聚合窗口模型
  - 统计信息：count, sum, min, max, avg, variance, std_dev
  - 异常分数统计：anomaly_count, anomaly_ratio

- [AnomalyAlert.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-cloud-common/src/main/java/com/amazonaws/services/common/model/AnomalyAlert.java) - 告警模型
  - 告警类型：单点、窗口、趋势、系统错误
  - 严重程度：LOW, MEDIUM, HIGH, CRITICAL

### 2. 工具层 (`edge-cloud-common`)

**核心类：**
- [BloomFilter.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-cloud-common/src/main/java/com/amazonaws/services/common/utils/BloomFilter.java) - 布隆过滤器去重
  - MurmurHash 哈希算法
  - 可配置误报率 (FPP)
  - 自动旋转机制

- [ClockDriftCorrector.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-cloud-common/src/main/java/com/amazonaws/services/common/utils/ClockDriftCorrector.java) - 时钟漂移校正
  - 滑动窗口样本收集
  - 中位数+标准差过滤异常值
  - 自适应偏移量计算

- [OfflineMessageStore.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-cloud-common/src/main/java/com/amazonaws/services/common/utils/OfflineMessageStore.java) - 断网补传
  - 本地文件持久化
  - 指数退避重试策略
  - TTL 过期清理
  - 存储空间限制

- [TwoPhaseCommitCoordinator.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-cloud-common/src/main/java/com/amazonaws/services/common/transaction/TwoPhaseCommitCoordinator.java) - 两阶段提交
  - PREPARE → COMMIT 两阶段
  - 参与者注册与状态追踪
  - 事务超时处理

### 3. 边缘处理层 (`edge-processing`)

**核心类：**
- [EnhancedStreamManagerSource.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-processing/src/main/java/com/amazonaws/services/edge/streammanager/EnhancedStreamManagerSource.java) - 增强数据源
  - 断点续读（基于 checkpoint）
  - 时钟漂移检测与校正
  - 序列 gap 检测
  - BloomFilter 去重

- [EdgeAnomalyDetectionFunction.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-processing/src/main/java/com/amazonaws/services/edge/operators/EdgeAnomalyDetectionFunction.java) - 边缘异常检测
  - 三组测量变量独立 RCF 模型
  - 增量学习更新
  - 低延迟实时评分

- [EdgePreAggregationFunction.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-processing/src/main/java/com/amazonaws/services/edge/operators/EdgePreAggregationFunction.java) - 边缘预聚合
  - 时间窗口聚合
  - BloomFilter + 时间窗口双重去重
  - 序列完整性校验
  - 统计特征计算

- [EnhancedStreamManagerSink.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-processing/src/main/java/com/amazonaws/services/edge/streammanager/EnhancedStreamManagerSink.java) - 增强数据输出
  - 事务性写入
  - 断网自动降级到本地存储
  - 网络恢复自动补传
  - 优先级队列管理

- [EdgeAnomalyProcessingJob.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/edge-processing/src/main/java/com/amazonaws/services/edge/EdgeAnomalyProcessingJob.java) - 边缘主作业

### 4. 云端处理层 (`cloud-processing`)

**核心类：**
- [CloudDeduplicationFunction.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/cloud-processing/src/main/java/com/amazonaws/services/cloud/operators/CloudDeduplicationFunction.java) - 云端去重
  - 幂等键持久化追踪（24小时 TTL）
  - 窗口级去重
  - 乱序检测

- [CloudSecondaryScoringFunction.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/cloud-processing/src/main/java/com/amazonaws/services/cloud/operators/CloudSecondaryScoringFunction.java) - 云端二次评分
  - 增强特征向量（统计特征+边缘分数+上下文）
  - 更大规模 RCF 模型
  - 自适应阈值（基于历史百分位）
  - 告警抑制（冷却时间机制）

- [AlertNotificationSink.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/cloud-processing/src/main/java/com/amazonaws/services/cloud/notification/AlertNotificationSink.java) - 告警通知
  - 多通道支持：Console, Log, SNS, CloudWatch
  - 可配置严重度路由
  - 告警冷却抑制
  - 批量发送优化

- [CloudAnomalyProcessingJob.java](file:///home/huangli/ws/bytedance/solo0601/HLGO-003/HLGO-003-3/src/cloud-processing/src/main/java/com/amazonaws/services/cloud/CloudAnomalyProcessingJob.java) - 云端主作业

## Exactly-Once 语义保证机制

### 1. 数据源端
- **断点续读**: Checkpoint 中持久化最后处理的序列号
- **幂等读取**: 基于序列去重，避免重复消费

### 2. 处理端
- **Flink Checkpointing**: 每 60 秒做一次全局状态快照
- **状态管理**: 所有算子状态纳入 Flink 管理
- **Exactly-Once Checkpoint Mode**: 保证状态一致性

### 3. 输出端
- **两阶段提交**: 预写日志 + 原子提交
- **幂等写入**: 基于 idempotency_key 去重
- **事务确认**: 完整确认后才标记完成

### 4. 传输端
- **StreamManager 持久化**: 边缘侧文件持久化
- **断网补传**: 网络恢复后自动重试
- **云端二次去重**: 基于幂等键再次校验

## 断网补传机制

```
网络正常 → 直接写入 StreamManager → 导出到 Kinesis
    │
    └─ 网络异常 → 写入 OfflineMessageStore → 本地文件持久化
                 │
                 └─ 后台重试线程 → 检测网络恢复 → 批量补传 → 删除本地副本
```

**重试策略：**
- 基础延迟：1秒
- 指数退避：2^retryCount
- 最大延迟：60秒
- 最大重试：5次
- TTL：24小时

## 时钟漂移校正

```
边缘消息（t_edge）→ 云端接收（t_cloud）→ 计算偏移量 → 滑动窗口过滤 → 计算中位数偏移
    │
    └─ 校正公式: t_corrected = t_edge + offset_median
```

**算法特点：**
- 滑动窗口：最近 100 个样本
- 异常值过滤：2σ 外的样本被排除
- 中位数估计：抗异常值干扰
- 定期同步：每 60 秒重新校准

## 重复消息抑制

### 三层去重架构

1. **边缘第一层（BloomFilter）**
   - 容量：100,000 条
   - 误报率：0.01%
   - 时间窗口：1小时 TTL

2. **边缘第二层（时间窗口）**
   - 相同内容 + 时间差 < 1s → 判定为重复
   - 基于序列 gap 检测

3. **云端第三层（幂等键）**
   - 持久化存储所有 idempotency_key
   - TTL：24小时
   - 支持事务级去重

## 告警引擎

### 告警类型
- `SINGLE_POINT`: 单点异常
- `WINDOW_AGGREGATE`: 窗口聚合异常
- `TREND_DETECTION`: 趋势异常
- `SYSTEM_ERROR`: 系统错误

### 严重程度分级
| 级别 | 分数阈值倍数 | 通知渠道 |
|------|-------------|----------|
| LOW | 1.0-1.5x | Log, Console |
| MEDIUM | 1.5-2.0x | + CloudWatch |
| HIGH | 2.0-3.0x | + SNS |
| CRITICAL | >3.0x | 全部渠道 |

### 告警抑制
- 同类告警 60 秒冷却期
- 低级别告警 5 分钟冷却期
- 按设备+分组+严重程度独立追踪

## 构建与部署

### 编译
```bash
cd src
mvn clean package -DskipTests
```

### 运行测试
```bash
cd src/edge-cloud-common
mvn exec:java -Dexec.mainClass="com.amazonaws.services.test.EndToEndIntegrationTest"
```

### 边缘端运行
```bash
java -jar edge-processing/target/edge-processing-1.0.0-SNAPSHOT.jar \
  --region eu-central-1 \
  --ggStreamHost localhost \
  --ggStreamPort 8089
```

### 云端运行
```bash
# 部署到 Kinesis Data Analytics 或本地运行
java -jar cloud-processing/target/cloud-processing-1.0.0-SNAPSHOT.jar \
  --KinesisRegion eu-central-1 \
  --InputStreamName tep-ingest-greengrass
```

## 配置参数

### 边缘端配置 (`application.properties`)
- `windowSizeMs`: 窗口大小（默认 60000ms）
- `RcfNumberOfTrees`: RCF 树数量（默认 50）
- `offlineStoragePath`: 断网存储路径

### 云端配置 (`application.properties`)
- `AnomalyThreshold`: 异常阈值（默认 1.5）
- `CloudRcfNumberOfTrees`: 云端 RCF 树数量（默认 100）
- `enableSns`: 是否启用 SNS 通知
- `SnsTopicArn`: SNS 主题 ARN

## 项目结构

```
src/
├── pom.xml                          # 父 POM
├── edge-cloud-common/               # 公共模块
│   ├── pom.xml
│   └── src/main/java/com/amazonaws/services/common/
│       ├── model/                    # 数据模型
│       ├── utils/                    # 工具类
│       ├── transaction/              # 事务管理
│       └── test/                     # 测试类
├── edge-processing/                  # 边缘处理模块
│   ├── pom.xml
│   └── src/main/java/com/amazonaws/services/edge/
│       ├── operators/                # 边缘算子
│       └── streammanager/            # StreamManager 集成
└── cloud-processing/                 # 云端处理模块
    ├── pom.xml
    └── src/main/java/com/amazonaws/services/cloud/
        ├── operators/                # 云端算子
        └── notification/             # 通知系统
```
