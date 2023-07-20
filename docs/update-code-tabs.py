#!/usr/bin/python

# Usage: python update-code-tabs.py file.md
# Iterate: for k in **/*md; do python update-code-tabs.py $k; done

import os
import sys


def update_code_tabs(content):

    stripped_content = [x.rstrip() for x in content]

    # move on if this file doesn't have code tabs
    if "<!--DOCUSAURUS_CODE_TABS-->" not in stripped_content:
        return content

    print("Replacing code tabs in file...")

    # add the import statements
    stripped_content.insert(5, "import Tabs from '@theme/Tabs';")
    stripped_content.insert(6, "import TabItem from '@theme/TabItem';\n")

    # replace the start of the Tabs block
    stripped_content = [x.replace(
        '<!--DOCUSAURUS_CODE_TABS-->', '<Tabs>') for x in stripped_content]

    # replace the end of the Tabs block
    stripped_content = [x.replace(
        '<!--END_DOCUSAURUS_CODE_TABS-->', '</TabItem>\n</Tabs>') for x in stripped_content]

    # iterate over code tabs; this script is not smart enough to be block specific
    tab_count = 0
    for i, line in enumerate(stripped_content):
        if line.startswith('<!--'):

            # get the string in the tab label
            tab_label = line.strip('<!--').strip('-->')

            # create the new tab object
            # a blank line after TabItem is important
            new_tag = f'<TabItem value="{tab_count}" label="{tab_label}">\n'

            if tab_count > 0 and stripped_content[i-1] != '<Tabs>' and stripped_content[i-2] != '<Tabs>':
                new_tag = '</TabItem>\n' + new_tag

            # remove the old tag and add the new tag
            stripped_content.pop(i)
            stripped_content.insert(i, new_tag)

            tab_count += 1

    # add newlines back
    new_content = [x + '\n' for x in stripped_content]

    return new_content

def main():
    fname = sys.argv[1]
    if not os.path.exists(fname):
        sys.exit(f"File '{fname}' does not exist. Exiting.")

    # print status
    print(f"\nWorking on '{fname}'")

    # read the existing doc
    with open(fname, 'r') as f:
        old_content = f.readlines()

    # process and update code tab lines
    new_content = update_code_tabs(old_content)

    # write updated doc
    with open(fname, 'w') as f:
        f.writelines(new_content)

if __name__ == "__main__":
    main()