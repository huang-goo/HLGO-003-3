---
name: java-maven-testing-conventions
description: Java Maven 项目中测试文件的标准位置、框架选择和目录结构规范
source: auto-skill
extracted_at: '2026-06-21T06:55:31.367Z'
---

# Java Maven 项目测试规范

在 Java Maven 项目中实现测试时，必须遵循以下规范以确保测试能被自动化工具正确识别和执行。

## 核心规则

### 1. 测试文件位置

测试类必须放在标准的 Maven 测试目录下：

```
src/
├── main/
│   └── java/           # 生产代码
└── test/
    └── java/           # 测试代码 ← 必须放这里
```

错误做法：
- 将测试类放在 src/main/java 下
- 将测试类放在生产代码包的 test 子包中（如 com.example.test）

正确做法：
- 测试类放在 src/test/java 下
- 测试包结构与被测类包结构一致

### 2. 测试框架选择

必须使用标准测试框架，而非 main 方法：

| 推荐 | 不推荐 |
|------|--------|
| JUnit 4/5 | main 方法 |
| TestNG | System.out.println |
| Mockito | 手动断言 |

### 3. Maven Surefire 插件

Maven Surefire 插件会自动识别以下模式的测试类：
- `**/Test*.java`
- `**/*Test.java`
- `**/*Tests.java`
- `**/*TestCase.java`

### 4. pom.xml 依赖配置

避免使用 systemPath 依赖本地 jar 文件：

```xml
<!-- 不推荐：可移植性差 -->
<dependency>
    <groupId>com.example</groupId>
    <artifactId>my-lib</artifactId>
    <scope>system</scope>
    <systemPath>${project.basedir}/lib/my-lib.jar</systemPath>
</dependency>

<!-- 推荐：使用 Maven 仓库或 install 本地仓库 -->
<dependency>
    <groupId>com.example</groupId>
    <artifactId>my-lib</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 验证清单

在宣称测试完成前，必须执行以下验证：

1. 运行 `mvn test` 确认测试被发现并执行
2. 检查测试输出是否包含 "Tests run: X"
3. 确认测试覆盖率报告生成（如有配置）
4. 验证测试类使用 @Test 注解而非 public static void main

## 典型错误案例

### 案例1：测试文件放错位置

```java
// 错误：放在 src/main/java
package com.example.test;

public class MyTest {
    public static void main(String[] args) {
        // 测试代码...
    }
}

// 正确：放在 src/test/java
package com.example;

import org.junit.Test;
import static org.junit.Assert.*;

public class MyTest {
    @Test
    public void testSomething() {
        // 测试代码...
    }
}
```

### 案例2：systemPath 依赖

```xml
<!-- 错误：Maven 警告 "should not point at files within the project directory" -->
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>greengrass-sdk</artifactId>
    <scope>system</scope>
    <systemPath>${project.basedir}/../lib/greengrass-sdk.jar</systemPath>
</dependency>

<!-- 正确：使用 Maven 仓库依赖或 mvn install:install-file -->
```

## 检查命令

```bash
# 验证测试是否被识别
mvn test -q 2>&1 | grep -E "(Tests run|No tests to run)"

# 检查测试文件位置
find . -name "*Test.java" -path "*/src/test/*"

# 验证 systemPath 依赖
grep -r "systemPath" pom.xml
```
