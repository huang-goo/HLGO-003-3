---
name: code-generation-quality-check
description: AI 生成 Java 代码时的自检清单，避免编译错误和结构问题
source: auto-skill
extracted_at: '2026-06-21T06:55:31.367Z'
---

# 代码生成质量自检清单

在生成 Java 代码（尤其是复杂系统如 Flink/Spark 项目）时，遵循以下自检流程以减少编译错误。

## 生成前检查

### 1. 理解项目结构

在修改或创建文件前：
- 使用 glob 或 find 命令确认目标文件是否存在
- 读取现有文件了解包结构和命名规范
- 检查 pom.xml 了解依赖版本和配置

### 2. 依赖版本确认

在使用框架 API 前：
- 确认 pom.xml 中的框架版本（如 Flink 1.13 vs 1.16 API 差异）
- 检查该版本是否支持目标 API（如 StateTtl 在旧版本可能不支持）
- 验证导入的类和方法在该版本中是否存在

## 生成中检查

### 3. 类完整性检查

创建新类时确保：
- 所有字段都有对应的 getter/setter（如果被其他类访问）
- 构造函数参数与字段匹配
- 实现了必要的接口方法
- 添加了 serialVersionUID（如果实现 Serializable）

### 4. 类型兼容性检查

使用框架 API 时：
- 检查方法参数类型是否匹配（如 long vs Long）
- 验证泛型类型参数
- 确认返回值类型处理正确

### 5. 导入完整性检查

确保所有使用的类都有正确导入：
- 检查是否使用了不存在的类
- 验证包路径正确
- 避免循环依赖

## 生成后检查

### 6. 编译验证

代码生成后必须：
```bash
# 1. 编译检查
mvn compile 2>&1 | grep -E "(ERROR|WARNING)"

# 2. 完整构建
mvn clean install -DskipTests

# 3. 测试执行
mvn test
```

### 7. 代码审查检查

- 检查是否有 TODO/FIXME 标记未完成
- 验证异常处理是否完整
- 确认日志记录是否恰当
- 检查资源释放（如 StreamManagerClient）

## 常见错误模式

### 错误1：缺少 getter/setter

```java
// 问题：其他类调用 getMetadata() 但该方法不存在
public class AggregatedWindow {
    private Map<String, String> metadata;
    // 缺少 getter/setter
}

// 修复：添加完整访问方法
public Map<String, String> getMetadata() {
    return metadata;
}
public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
}
```

### 错误2：API 版本不兼容

```java
// 问题：Flink 1.13 可能不支持 StateTtl
StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .build();

// 修复：移除 StateTtl 或使用兼容方式
ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>("name", T.class);
```

### 错误3：类型不匹配

```java
// 问题：setTime() 接受 long 但传入了其他类型
point.setTime(System.currentTimeMillis()); // 正确
point.setTime((int) System.currentTimeMillis()); // 错误：可能溢出

// 修复：确保类型匹配
long timestamp = System.currentTimeMillis();
point.setTime(timestamp);
```

## 调试技巧

当编译失败时：

1. **查看完整错误信息**：
   ```bash
   mvn compile 2>&1 | tail -50
   ```

2. **定位错误文件**：
   - 错误信息通常包含 `[ERROR] /path/to/File.java:[行号,列号]`

3. **检查相关文件**：
   - 错误可能源于被调用的类缺少方法或字段

4. **逐个修复**：
   - 每次只修复一个错误，重新编译验证

## 快速参考

| 问题类型 | 检查方式 | 修复方法 |
|---------|---------|---------|
| 方法不存在 | 搜索类定义 | 添加方法实现 |
| 类型不匹配 | 检查方法签名 | 调整参数类型 |
| 导入缺失 | 查看编译错误 | 添加 import 语句 |
| 接口未实现 | 检查 implements | 添加接口方法 |
| 版本不兼容 | 检查 pom.xml | 使用兼容 API |
