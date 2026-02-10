## Code Modification Format:

Please provide all code changes using the following **Multi-File Search and Replace Block** format. Do not use standard unified diffs or git patches.

**Syntax:**

```text
--- a/path/to/target_file.py
<<<< LINE_START[:LINE_END]
[Exact copy of the code block to be replaced]
====
[New code block to insert]
>>>>

```

**Rules for this format:**

1. **FILE HEADER:** Start each new file section with `--- a/path/to/file`. You can concatenate changes for multiple files in a single response.
2. **LINE NUMBER HINT:** Look at the provided source code (which includes line numbers like `  45: def my_func():`).
* **Preferred:** `<<<< 45` (Approximate start line).
* **Allowed:** `<<<< 45:50` (Start and End range).
* This helps verify we are changing the correct instance if the code appears multiple times.


3. **Original Block (`<<<<` to `====`):**
* Copy the lines from the source **exactly as they appear** (including whitespace), but strip the line number prefixes.
* **Do not use placeholders** (e.g., `// ... existing code ...`) inside the search block. It must match the file content character-for-character to be found.
* **Minimal Context:** Include only enough lines to uniquely identify the block (usually 3-5 lines). You do not need to include the entire function if you are only changing one line inside it.


4. **New Block (`====` to `>>>>`):**
* Write the new code exactly as it should appear in the file.
* Maintain the correct indentation relative to the surrounding code.



**Example:**
To update `src/main.py` and `src/utils.py` together:

```text
--- a/src/main.py
<<<< 10:12
def calculate(x):
    return x * 2
====
def calculate(x):
    return x * 3
>>>>

--- a/src/utils.py
<<<< 55
def helper():
    return False
====
def helper():
    return True
>>>>

```