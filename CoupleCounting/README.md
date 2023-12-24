Spark program that reads in a folder of files and searches each file for pairs of words that occur together on the same line. It counts the number of times where each pair occurs together across the files. The goal is to print the top 10 such pairs. We don't care about capitalization or punctuation. A pair $(u, v)$ is conisered the same as the pair $(v, u)$. Here is an example:
```
input:
Ho Ho Ho! I am in the chimney. Ho Ho Ho !
chim chimney chimney chim

intermediate pairs:
(ho, ho) (ho ho) (ho, i) (i, am) (am, in) (in, the) (the chimney)
(chimney. ho) (ho, ho) (ho, ho)
(chim, chimney) (chimney, chimney) (chimney, chim)

result:

((ho,ho), 4)
((chim, chimney), 2) <-- (chim, chimney) and (chimney, chim) are the same
the rest of the pairs are 1's
```
