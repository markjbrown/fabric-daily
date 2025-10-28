# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
%pip install python-dotenv==1.0.1
%pip install gradio


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import gradio as gr
import os
from dotenv import load_dotenv

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def greet(name):
    return f"Hello, {name}!"

with gr.Blocks() as demo:
    gr.Markdown("### Simple Test UI")
    with gr.Row():
        name_input = gr.Textbox(label="Enter your name")
        greet_button = gr.Button("Greet")
    output = gr.Textbox(label="Output")

    greet_button.click(fn=greet, inputs=name_input, outputs=output)

demo.launch(share=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
