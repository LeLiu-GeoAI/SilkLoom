# 06 多行结果：GIS术语提取

这个示例用于演示“单行文本输入 -> 多行术语输出”。

## 关键配置

- `task.nested_target_field: terms`
- 模型返回 JSON 顶层必须包含 `terms` 数组。
- `terms` 数组中的每个对象字段对应 `target_schema`。

## 运行方式

1. 在 SilkLoom 中选择本目录下 `data.csv`。
2. 点击“导入配置”，选择本目录下 `task_config.yml`。
3. 配置模型后运行。
4. 导出结果可看到同一条输入被展开为多条术语记录。
