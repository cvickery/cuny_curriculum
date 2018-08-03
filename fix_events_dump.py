# Convert integer group numbers to reals with default fraction part of 0.1 in a dump of the events
# table. Read the dump file from stdin; write the converted file to stdout.

import re
import sys
for line in sys.stdin:
  # If a line starts with a digit (event.id) and the second number on the line is an int (group
  # number), add ".1" to the int.
  if line[0].isdigit():
    match = re.search('\t([\d\.]+)\t', line)
    group = float(match[1])
    if group == int(group):
      group += 0.1
      line = re.sub('\t[0-9\.]+\t', '\t{:.1f}\t'.format(group), line, 1)
  print(line.strip('\n'))
