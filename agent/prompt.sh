#!/bin/bash

# Check if an output filename was provided
if [ -z "$1" ]; then
    echo "Usage: $0 <output_file.md>"
    exit 1
fi

OUTPUT_FILE="$1"
TEMPLATE="./agent/coding_agent.template.md"
AGENT_DIR="./agent"

python3 - <<EOF
import os

template_path = "$TEMPLATE"
output_path = "$OUTPUT_FILE"
agent_dir = "$AGENT_DIR"
placeholders = ["x.md", "code_modification_format.md", "current_status.md"]

try:
    with open(template_path, 'r') as f:
        content = f.read()

    for filename in placeholders:
        placeholder = f"{{{{{filename}}}}}"
        file_path = os.path.join(agent_dir, filename)
        
        if os.path.exists(file_path):
            with open(file_path, 'r') as f_in:
                replacement = f_in.read()
            content = content.replace(placeholder, replacement)
        else:
            print(f"Warning: {file_path} not found.")

    with open(output_path, 'w') as f_out:
        f_out.write(content)
    print(f"Success: {output_path} has been generated.")

except Exception as e:
    print(f"Error: {e}")
EOF