# Text conventions

The database contains many text fields for comments, citation titles, quotations from
sources, and similar purposes. There are a few common conventions for these text fields:

- The text is written using Markdown, as rendered by
  [react-markdown](https://github.com/remarkjs/react-markdown). However, Markdown
  formatting is usually kept to a minimum; most commonly used are underscores for
  italics, and occasionally hyperlinks.
- The degree symbol for geographic coordinates (°) may be replaced with "\*".
- The male (♂) and female (♀) symbols may be replaced with "[M]" and "[F]",
  respectively.
- Text within square brackets is generally an interpolation containing comments that
  were not in the original source. If the original text contained square brackets, "@"
  may be added at the end of the string.
- Some sources use superscript and subscript to identify tooth positions in the upper
  and lower jaw. I instead always use uppercase and lowercase ("M1" or "m1"), even when
  the source uses a different convention.
- If the source correctly contains a hyphen followed by a space, a backslash (\\) should
  be added before the space. This allows automatically cleaning up hyphens that are the
  result of line breaks copy-pasted from a source.
- References to articles by filename, like "Agathaeromys nov.pdf" within curly braces,
  are turned into links to the corresponding article during rendering.
- Similarly, strings of the form "n/1234" in curly braces become links to Name #1234.
