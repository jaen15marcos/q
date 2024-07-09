/
Matrices/Vectors/Symbols/Strings/Table/Functions
\

/1) Create an n x n matrix of random floats between a and b
5 5#0.1+25?1.0 /n=5; range between 0.1 and 1.0
{[n;a;b] (n;n) #a+(n*n)?b}[5;0;1f]

/2) Isolate the ith row or jth column
matrix: 5 5#0.1+25?1.0
matrix[1] /row 2 as index starts at 0
matrix[;1] /col 2

/3) Isolate the leading diagonal (top left to bottom right)
matrix @' til count(matrix)

/4) Create an identity matrix
{0 1f x =/: x}til 10 

/5) Find the transpose
flip matrix 

/6) Sort each row/column in ascending order
{[matrix]{asc x }each matrix}[matrix] /each row asc
{[matrix] flip {asc x }each flip matrix}[matrix] /each col asc

/7) Find the index of the max/min element in each row

{[matrix]{x ? min x}each matrix}[matrix] /each row min
{[matrix]{x ? min x}each flip matrix}[matrix] /each col min

/8) Generate a list of random 3-character symbols
{`$3?.Q.a}each til 5

/9) Convert the symbols `rix`mat to the symbol `matrix
`$"" sv (string`mat; string`rix)

/10) Replace every letter “a” in a character vector with a space

names: {10?.Q.a}each til 10 /generate character vectors
{[names] {ssr[x;"a";" "]} each names}[names]

/11) Replace every 5 th character in a character vector with a space
{[names;n]@[names; -1+n*1+til count[names] div n;:;" "]}[;5]each names

/12) Are x and y permutations of each other (eg: 2 5 8 9 and 2 5 9 8 are!)
{[x;y] (asc x)~asc y}[2 5 8 9; 2 5 9 8]

/13) Find all the permutations 
{[N;l] $[N=1;l; raze .z.s[N-1;l]{x,/: y except x}\:l]}[3; 2 5 8 9]

/14) Find all the combinations
perm: {[N;l] $[N=1;l; raze .z.s[N-1;l]{x,/: y except x}\:l]}[3; 2 5 8 9]
distinct asc each perm

/15) Write a function to generate the lists “a”, “ab”, “abc”, etc from the input .Q.a
{(1+til count(x))#\:x}[.Q.a]

/16) Write a function which operates on its results n times. Eg. Let the function be f:{(x+y;x-y)} then f[2;3]= 5 -1 ----&gt; f[5;-1]= 4 6 etc
2 {(x[0]+x[1]),(x[0]-x[1])}/2 3

/17) Write a function which finds the maximum sum of a row, column or main diagonal across any nxn matrix.
row_max: max {sum x}each matrix
col_max: max {sum x}each flip matrix
diagonal_sum: sum matrix @' til count(matrix)
/18) Write a function which changes the textual instructions “R3,U4,L1” etc into the following vector list: (1 0;1 0;1 0;0 1;0 1;0 1;0 1;-1 0). If you start at (0,0) what simple function allows you to find every coordinate visited?

/19) Given a string x and a character y, how many times does y occur in x
{[x;y] sum x=y}["fhqwhgads";"h"] 

/20) Given a string x, is it identical when read forwards and backwards?
{[x] (reverse x)~x}["racecar"] 

/21) Given a string x, produce a list of characters which appear more than once in x
{[x] where 1< count each group x}["applause"]  /group x yields the unique character values and their index

/22) Given strings x and y, do both strings contain the same letters, possibly in a different order?
{[x;y] (asc x)~(asc y)}["teapot";"toptea"]

/23) Given a string x, find all the characters which occur exactly once, in the order they appear.
{[x] where 1=count each group x}["somewhat heterogenous"]

/24) Given strings x and y, is x a rotation of the characters in y?
{[x;y] x in (1 rotate) scan y}["foobar";"barfoo"]

/25) Given a list of strings x, sort the strings by length, ascending.

{x iasc count each x}[("books";"apple";"peanut";"aardvark";"melon";"pie")] 
/iasc: Where x is a list or dictionary, returns the indexes needed to sort list x in ascending order.

/26) Given a string x, identify the character which occurs most frequently. 
/If more than one character occurs the same number of times, you may choose arbitrarily.
x: "abdbbac"
first key desc count each group x

(raze/) x where (value count each group x)=(max desc count each group x) /select all characters that occur max amount of times 

/27)  Given a string x consisting of words (one or more non-space characters) which are separated by spaces, reverse the order of the characters in each word.
x: "a few words in a sentence"
"a wef sdrow ni a ecnetnes"
reverse each " "vs x /separate strings by spaces and reverses them 
" "sv reverse each " "vs x /join strings in list by adding spaces to them 
{" "sv reverse each " "vs x} 
/sv: partition a symbol, string, or bytestream, ex: "," vs "one,two,three" yields "one" "two" "three"
/vs: join a symbol, string, or bytestream, ex: "," sv ("one";"two";"three") yields "one,two,three"

/28)  Given a string x and a boolean vector y of the same length, extract the characters of x corresponding to a 1 in y
f["foobar";100101b]
{[x;y] x where y}["foobar";100101b]

/29) Given a string x and a boolean vector y, spread the characters of x to the positions of 1s in y, filling intervening characters with underscores.
f["bigger";0011110011b]
{("_",x)y*sums y}["bigger";0011110011b] /y*sums y gets the indexes where one adds _. ("_",x) adds "_" where index is 0
/("_","bigger")0 0 1 1 1 1 0 0 1 1 yields "__bbbb__bb"
/("_","bigger")0 0 1 2 3 4 0 0 5 6i yields "__bigg__er"

/30)  Given a string x, replace all the vowels (a, e, i, o, u, or y) with underscores.

{@[x; where x in "AEIOUYaeiouy";:;"_"]}["FLAPJACKS"]

/31) Given a string x, remove all the vowels entirely.
"Several normal words" except "AEIOUYaeiouy"

/32) Given a string x consisting of words separated by spaces (as above), and a string y, replace all words in x which are the same as y with a series of xs.
{ssr[x;y;count[y]#"X"]}["a few words in a sentence";"words"]

/33) Given a string x, generate a list of all possible re-orderings of the characters in x

{[N;l] $[N=1;l; raze .z.s[N-1;l]{x,/: y except x}\:l]}[3; "xyz"]
{(1 0#x) {raze({raze reverse 0 1 _ x}\)each x,'y}/ x} / (AR)
/f["a few words in a sentence";"words"] = "a few XXXXX in a sentence"

/34) Eliminate white spaces in string 
trim x /"   abc def  "
{{y _ x}/[x;] 1 -1*?'[;0b]1 reverse\null x}["   abc def  "] /alternative to trim

/35) Newton-Raphson
10 {[xn] xn - ((xn*xn)-2)%(2*xn)}/2

/KDB/Q Exercises (https://qkdb.wordpress.com/)
/1. Armstrong number: An n-digit number equal to the sum of the nth powers of its digits.
/EX1: 123 is a three-digit number, raise each digit to the third power, and add: 1^3 + 2^3 + 3^3 = 36, which shows that it is not an Armstrong number number.
/EX2: 1634 is Armstrong number as 14 + 64 + 34 + 44 = 1634.
(1 xexp 3) + (2 xexp 3) + (3 xexp 3)

isArmsNum:{[no] no=sum {[c;n] ("I"$n) xexp c }[count string no]each string no}
isArmsNum 1634

/get all Armstrong numbers up to 10k
(2 + til 10000000) where (isArmsNum'[2 + til 10000000])

/2. Fibonacci sequence: xn = xn-1 + xn-2, where x0 = 0, x1= 1
fibonnaci: {[N;l] N {x, sum -2#x}/l}[N; 1 2] 
sum fibonnaci where not fibonnaci mod 2

fibSeq:-1_{x,sum reverse[x]0 1}/[{(4000000)>last x};1 2]
sum fibSeq where not fibSeq mod 2
meta getData.edwQ

//Challenge: Each new term in the Fibonacci sequence is generated by adding the previous two terms. By starting with 1 and 2, the first 10 terms will be:
/By considering the terms in the Fibonacci sequence whose values do not exceed four million, find the sum of the even-valued terms.
fibSeq:-1_{x,sum reverse[x]0 1}/[{(4000000)>last x};1 2]
sum fibSeq where not fibSeq mod 2
/or 
fibonnaci: {[N;l] N {x, sum -2#x}/l}[N; 1 2] /N=30
sum fibonnaci where not fibonnaci mod 2

/3. Find the greatest product of five consecutive digits in the 1000-digit number.

number: raze string 1000?1 + 10 /create number
number: ((1000 - count[number])_number) /ensures there are 1000 digits
max {prd{"I"$ x}each number x+til 5}each til 1000 /prd stands for product
{{"I"$ x}each number x+til 5}each til 1000 /gets cosecutive digits in list form 

/{t:.z.p;while[.z.p<t+x]} 00:00:05 /sleep 5 secs

/Tables
/Run the following to generate a trades, quotes and reference table:
/q:`ticker`venue xcols update venue:(`AAPL`GOOGL`MSFT!`OQ`N`Z)[ticker] from ([]ticker:100?`AAPL`GOOGL`MSFT;time:asc 100?.z.t;bs:1+0N?100;bp:100?100f;ap:100?100f;as:1+0N?100)
/t:@[;`time;`s#]delete bs,bp,ap,as from (update time:time+100?00:00:01.000,ts:1+100?100,tp:?[100?0b;.01*&quot;i&quot;$100*q `bp;.01*&quot;i&quot;$100*q `ap] from
/q) ref:([ticker:`GOOGL`MSFT]vendor:`Google`Microsoft)
/Tasks
/1) Find the spread (difference between bp and ap) for each quote.
/2) Find the 5 rows with the greatest spread
/3) Find the greatest spread for each ticker
/4) Find the average spread for each ticker
/5) Concatenate each ticker and venue with a dot and use this to key the quotes table (Eg.`AAPL.OQ)
/6) Join the trades and reference tables so that all columns remain
/7) Join the trade and reference tables so that only columns with known vendors remain
/8) Use an asof join to join the trades and quotes tables
/9) Use this table to decide how many trades were sells (caused by bids) and how many were buys (caused by asks)

/Attributes and saving to disk
/1) Describe each of the attributes used in kdb
/2) Describe an example of when an attribute would fail
/3) What ways can a table be saved / What is the structure of saved tables?
/4) What does .Q.en do and what variable and file does it create?
/5) How are tables splayed and what does the .d file contain?
/6) What would the following statement do: `:trades/.d set get [`:trades/.d] except `price
/7) For the .Q.dpft operator what are the four arguments d,p,f and t?
/IPC
/1) How do you open a handle and what is returned?
/2) On the server side what does .z.pw do?
/3) On the server side the .z.po (port open event) contains the variables .z.u,.z.a,.z.w. What are they?
/4) What does a negative handle do?
/5) On the server the .z.pg (port get) and .z.ps (port set), and others, deal with queries. How do they do this? (Hint: the argument is the query itself)
/6) How do you close a handle and what server-side event is triggered?

Challenge:
Expect a challenge on functions. This could be anything from generating a Fibonacci sequence to
finding a path through a matrix. You do not necessarily need to complete the challenge but if
possible be vocal about how you might attempt it.

/1) Write a function to generate the lists “a”, “ab”, “abc”, etc from the input .Q.a
/2) Write a function which operates on its results n times. Eg. Let the function be f:{(x+y;x-y)} then f[2;3]= 5 -1 ----&gt; f[5;-1]= 4 6 etc
/3) Write a function which finds the maximum sum of a row, column or main diagonal acrossnany nxn matrix.
/4) Write a function which changes the textual instructions “R3,U4,L1” etc into the following vector list: (1 0;1 0;1 0;0 1;0 1;0 1;0 1;-1 0). If you start at (0,0) what simple function allows you to find every coordinate visited?
/5) Write a function which finds the number of ways to make 5p (using 1p,2p,5p only)
/6) Find the minimum path from top to bottom through this matrix given that you can only move to the adjacent square diagonally left or diagonally right. It may help to use the mmax[2;] function: show m:(sums til 5)_15?10
