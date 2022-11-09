#! /usr/local/bin/python3

import sys


def smartify(dumb: str):
  """ If the dumb string contains straight double quotes, convert them to curly quotes.
      Alternates left & right curlies.
  """
  left = '“'
  right = '”'
  use_left = True
  smart = []
  for ch in dumb:
    if ch == '"':
      smart.append(left if use_left else right)
      use_left = not use_left
    else:
      smart.append(ch)
  return ''.join(smart)


if __name__ == '__main__':
  arg_str = ' '.join(sys.argv[1:])
  arg_str = smartify(arg_str)
  print(arg_str)
