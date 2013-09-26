module Console where

import Data.List(intercalate)

csi args code = concat ["\ESC[" ,  intercalate ";" (map show args), code ]

peach = setExtendedColor 202
azure = setExtendedColor  27

treset = "\ESC[m"

charNotEquals = '\x2260'
charCheck = '\x2714'
charLeftArrow = '\x2b05'
charRightArrow = '\x279c'

neq = concat [setAttr bold , " ", [charNotEquals], " " , treset]

setAttr x = csi [x] "m"
bold = 1
faint = 2
normal = 22
italicized = 3
notItalicized = 23
underlined = 4
doubleUnderlined = 21
notUnderlined = 24
slowBlink = 5
fastBlink = 6
noBlink = 25
invisible = 8
visible = 28
swapFgBg = 7
unswapFgBg = 27

dullBlack = 30
dullRed = 31
dullGreen = 32
dullYellow = 33
dullBlue = 34
dullMagenta = 35
dullCyan = 36
dullWhite = 37

vividBlack = 40
vividRed = 41
vividGreen = 42
vividYellow = 43
vividBlue = 44
vividMagenta = 45
vividCyan = 46
vividWhite = 47

hideCursor = "\ESC[?25l"
showCursor = "\ESC[?25h"

setTitle t = concat ["\ESC]0;",filter (/= '\007') t,"\007" ]

setColor x = setAttr x
setBackgroundColor x = setAttr (10+x)
setExtendedColor x = csi [38,5,x] "m"
setExtendedBackgroundColor x = csi [48,5,x] "m"
setRGB r g b = csi [38,2,r,g,b] "m"
setBackgroundRGB r g b = csi [48,2,r,g,b] "m"

cursorUp n = csi [n] "A"
cursorDown n = csi [n] "B"
cursorForward n = csi [n] "C"
cursorBackward n = csi [n] "D"

clearScreen = csi [2] "J"
clearToScreenBeginning = csi [1] "J"
clearToScreenEnd = csi [0] "J"

clearLine = csi [2] "K"
clearToLineBeginning = csi [1] "K"
clearToLineEnd = csi [0] "K"

setPosition n m = csi [n+1, m+1] "H"
setColumn n = csi [n+1] "G"
