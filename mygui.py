from nicegui import app
from nicegui.widgets import *

# 创建界面
with Grid():
    with HBox():
        source_db_label = Label("Source Database:")
        source_db_input = TextInput(placeholder="Enter source database connection info")

    with HBox():
        target_db_label = Label("Target Database:")
        target_db_input = TextInput(placeholder="Enter target database connection info")

    table_name_input = TextInput(placeholder="Enter table name")

submit_button = Button("Submit")

@app(submit_button.on_click)
def on_submit():
    # 当点击提交按钮时触发的函数
    source_db = source_db_input.value
    target_db = target_db_input.value
    table_name = table_name_input.value

    print(f"Source Database: {source_db}")
    print(f"Target Database: {target_db}")
    print(f"Table Name: {table_name}")

# 启动界面
app.exec()