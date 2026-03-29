# Parsing Gigabytes of JSON per Second

**Geoff Langdale** • **Daniel Lemire**

---

## Abstract

JavaScript Object Notation (JSON) is a ubiquitous data exchange format on the Web. Ingesting JSON documents can become a performance bottleneck due to the sheer volume of data. We are thus motivated to make JSON parsing as fast as possible.

Despite the maturity of the problem of JSON parsing, we show that substantial speedups are possible. We present the first standard-compliant JSON parser to process gigabytes of data per second on a single core, using commodity processors. We can use a quarter or fewer instructions than a state-of-the-art reference parser like RapidJSON. Unlike other validating parsers, our software (simdjson) makes extensive use of Single Instruction, Multiple Data (SIMD) instructions. To ensure reproducibility, simdjson is freely available as open-source software under a liberal license.

---

## 1 Introduction

JavaScript Object Notation (JSON) is a text format used to represent data [4]. It is commonly used for browser-server communication on the Web. It is supported by many database systems such as MySQL, PostgreSQL, IBM DB2, SQL Server, Oracle, and data-science frameworks such as Pandas. Many document-oriented databases are centered around JSON such as CouchDB or RethinkDB.

The JSON syntax can be viewed as a restricted form of JavaScript, but it is used in many programming languages. JSON has four primitive types or atoms (string, number, Boolean, null) that can be embedded within composed types (arrays and objects). An object takes the form of a series of key-value pairs between braces, where keys are strings (e.g., `{"name": "Jack", "age": 22}`). An array is a list of comma-separated values between brackets (e.g., `[1, "abc", null]`). Composed types can contain primitive types or arbitrarily deeply nested composed types as values. The JSON specification defines six structural characters ('[', '{', ']', '}', ':', ','): they serve to delimit the locations and structure of objects and arrays.

To access the data contained in a JSON document from software, it is typical to transform the JSON text into a tree-like logical representation, an operation we call JSON parsing. We refer to each value, object and array as a node in the parsed tree. After parsing, the programmer can access each node in turn and navigate to its siblings or its children without need for complicated and error-prone string parsing.

Parsing large JSON documents is a common task. Palkar et al. state that big-data applications can spend 80-90% of their time parsing JSON documents [25]. Boncz et al. identified the acceleration of JSON parsing as a topic of interest for speeding up database processing [2].

JSON parsing implies error checking: arrays must start and end with a bracket, objects must start and end with a brace, objects must be made of comma-separated pairs of values (separated by a colon) where all keys are strings. Numbers must follow the specification and fit within a valid range. Outside of string values, only a few ASCII characters are allowed. Within string values, several characters (like ASCII line endings) must be escaped. The JSON specification requires that documents use a unicode character encoding (UTF-8, UTF-16, or UTF-32), with UTF-8 being the default. Thus we must validate the character encoding of all strings. JSON parsing is therefore more onerous than merely locating nodes. Our contention is that a parser that accepts erroneous JSON is both dangerous—in that it will silently accept malformed JSON whether this has been generated accidentally or maliciously—and poorly specified—it is difficult to anticipate or widely agree on what the semantics of malformed JSON files should be.

To accelerate processing, we should use our processors as efficiently as possible. Commodity processors (Intel, AMD, ARM, POWER) support single-instruction-multiple-data (SIMD) instructions. These SIMD instructions operate on several words at once unlike regular instructions. For example, starting with the Haswell microarchitecture (2013), Intel and AMD processors support the AVX2 instruction set and 256-bit vector registers. Hence, on recent x64 processors, we can compare two strings of 32 characters in a single instruction. It is thus straightforward to use SIMD instructions to locate significant characters (e.g., '[', ']', '{', '}', ':', ',') using few instructions. We refer to the application of SIMD instructions as **vectorization**. Vectorized software tends to use fewer instructions than conventional software. Everything else being equal, code that generates fewer instructions is faster.

A closely related concept to vectorization is branchless processing: whenever the processor must choose between two code paths (a branch), there is a risk of incurring several cycles of penalty due to a mispredicted branch on current pipelined processors. In our experience, SIMD instructions are most likely to be beneficial in a branchless setting.

To our knowledge, publicly available JSON validating parsers make little use of SIMD instructions. Due to its complexity, the full JSON parsing problem may not appear immediately amenable to vectorization.

One of our core results is that SIMD instructions combined with minimal branching can lead to new speed records for JSON parsing—often processing gigabytes of data per second on a single core. We present several specific performance-oriented strategies that are of general interest.

- We detect quoted strings, using solely arithmetic and logical operations and a fixed number of instructions per input bytes, while omitting escaped quotes (§3.1.1).
- We differentiate between sets of code-point values using vectorized classification thus avoiding the burden of doing **N** comparisons to recognize that a value is part of a set of size **N** (§3.1.2).
- We validate UTF-8 strings using solely SIMD instructions (§3.1.5).

---

## 2 Related Work

A common strategy to accelerate JSON parsing in the literature is to parse selectively. Alagiannis et al. [1] presented NoDB, an approach where one queries the JSON data without first loading it in the database. It relies in part on selective parsing of the input. Bonetta and Brantner use speculative just-in-time (JIT) compilation and selective data access to speed up JSON processing [3]. They find repeated constant structures and generate code targeting these structures.

Li et al. present their fast parser, Mison which can jump directly to a queried field without parsing intermediate content [17]. Mison uses SIMD instructions to quickly identify some structural characters but otherwise works by processing bit-vectors in general purpose registers with branch-heavy loops. Mison does not attempt to validate documents; it assumes that documents are pure ASCII as opposed to unicode (UTF8). We summarize the most relevant component of the Mison architecture as follows:

1. In a first step, the input document is compared against each of the structural characters ('[', '{', ']', '}', ':', ',') as well as the backslash ('\'). Each comparison uses a SIMD instruction, comparing 32 pairs of bytes at a time. The comparisons are then converted into bitmaps where the bit value 1 indicate the presence of the corresponding structural character. Mison omits the structural characters related to arrays ('[', ']') when they are unneeded. Mison only uses SIMD instructions during this first step; it also appears to be the only step that is essentially branch-free.
2. During a second step, Mison identifies the starting and ending point of each string in the document. It uses the quote and backslash bitmaps. For each quote character, Mison counts the number of preceding backslashes using a fast instruction (popcnt): quotes preceded by an odd number of backslashes are turned off and ignored.
3. During a third step, Mison identifies the string spans delimited by the quotes. It takes each word (e.g., 32 bits) from the bitmap produced during the second step. It iteratively turns pairs of quotes into a string mask (where a 1-bit indicates the content of a string); using a small number of arithmetic and logical operations during each iteration.
4. In a final step, Mison uses the string masks to turn off and ignore structural characters (e.g., '{', '}', ':', ',') contained inside strings. Mison stores all opening braces in a stack. It pops the stack with each new closing brace, starting from the left, thus finding pairs of matching braces. For each possible nesting depth, a bitmap indicating the location of the colons can be constructed by partially copying the input colon bitmap.

Starting from the colon locations extracted from the bitmaps, Mison can parse the keys by scanning backward and the values by scanning forward. It can select only the content at a given depth. In effect, the colon bitmap serves as an index to selectively parse the input document. In some instances, Mison can scan through JSON documents at a speed of over 2 GB/s for high selectivity queries on a 3.5 GHz Intel processor. It is faster than what is possible with a conventional validating parser like RapidJSON.

FishStore [29] parses JSON data and selects subsets of interest, storing the result in a fast key-value store [6]. While the original FishStore relied on Mison, the open-source version uses simdjson by default for fast parsing.

Pavlopoulou et al. [26] propose a parallelized JSON processor that supports advanced queries and rewrite rules. It avoids the need to first load the data.

Sparser filters quickly an unprocessed document to find mostly just the relevant information [25], and then relies on a parser. We could use simdjson with Sparser.

Systems based on selective parsing like Mison or Sparser might be beneficial when only a small subset of the data is of interest. However, if the data is accessed repeatedly, it might be preferable to load the data in a database engine using a standard parser. Nonvalidating parsers like Mison might be best with tightly integrated systems where invalid inputs are unlikely.

### 2.1 XML Parsing

Before JSON, there has been a lot of similar work done on parsing XML. Noga et al. [24] report that when fewer than 80% of the values need to be parsed, it is more economical to parse just the needed values. Marian et al. [19] propose to "project" XML documents, down to a smaller document before executing queries. Green et al. [14] show that we can parse XML quickly using a Deterministic Finite Automaton (DFA) where the states are computed lazily, during parsing. Farfán et al. [10] go further and skip entire sections of the XML document, using internal physical pointers. Takase et al. [28] accelerate XML parsing by avoiding syntactic analysis when subsets of text have been previously encountered. Kostoulas et al. designed a fast validating XML parser called Screamer: it achieves higher speed by reducing the number of distinct processing steps [15]. Cameron et al. show that we can parse XML faster using SIMD instructions [5], in their parser (called Parabix). Zhang et al. [31] show how we can parse XML documents in parallel by first indexing the document, and then separately parsing partitions of the document.

Mytkowicz et al. [22] show how to vectorize finite-state machines using SIMD instructions. They demonstrate good results with HTML tokenization, being more than twice as fast as a baseline.

### 2.2 CSV Parsing

Data also comes in the form of comma-separated values (CSV). Mühlbauer et al optimize CSV parsing and loading using SIMD instructions to locate delimiters and invalid characters [20]. Ge et al. use a two-pass approach where the first pass identifies the regions between delimiters while the second pass processes the records [12].

---

## 3 Parser Architecture and Implementation

In our experience, most JSON parsers proceed by top-down recursive descent [7] that makes a single pass through the input bytes, doing character-by-character decoding. We adopt a different strategy, using two distinct passes. We briefly describe the two stages before covering them in detail in subsequent sections.

### 3.1 Stage 1: Structural and Pseudo-Structural Elements

The first stage of our processing must identify key points in our input: the structural characters of JSON (brace, bracket, colon and comma), the start and end of strings as delineated by double quote characters, other JSON atoms that are not distinguishable by simple characters (true, false, null and numbers), as well as discovering these characters and atoms in the presence of both quoting conventions and backslash escaping conventions.

In JSON, a first pass over the input can efficiently discover the significant characters that delineate syntactic elements (objects and arrays). Unfortunately, these characters may also appear between quotes, so we need to identify quotes. It is also necessary to identify the backslash character because JSON allows escaped characters: `\"`, `\\`, `\/`, `\b`, `\f`, `\n`, `\r`, `\t`, as well as escaped unicode characters (e.g. `\uDD1E`).

A point of reference is Mison [17], a fast parser in C++. Mison uses vector instructions to identify the colons, braces, quotes and backslashes. The detected quotes and backslashes are used to filter out the insignificant colons and braces. We follow the broad outline of the construction of a structural index as set forth in Mison; first, the discovery of odd-length sequences of backslash characters - which will cause quote characters immediately following to be escaped and not serve their quoting role but instead be literal characters, second, the discovery of quote pairs - which cause structural characters within the quote pairs to also be merely literal characters and have no function as structural characters, then finally the discovery of structural characters not contained within the quote pairs. We depart from the Mison paper in method and overall design. The Mison authors loop over the results of their initial SIMD identification of characters, while we propose branchless sequences to accomplish similar tasks. For example, to locate escaped quote characters, they iterate over the repeated quote characters. Their Algorithm 1 identifies the location of the quoted characters by iterating through the unescaped quote characters. We have no such loops in our stage 1: it is essentially branchless, with a fixed cost per input bytes (except for character-encoding validation, §3.1.5). Furthermore, Mison's vectorized processing is more limited by design as it does not identify the locations of the atoms, it does not process the white-space characters and it does not validate the character encoding.

#### 3.1.1 Identification of the quoted substrings

Identifying escaped quotes is less trivial than it appears. While it is easy to recognize that the string `"\""` is made of an escaped quote since a quote character immediately preceded by a backslash, if a quote is preceded by an even number of backslashes (e.g., `"\\""`), then it is not escaped since `\\` is an escaped backslash. We distinguish sequences of backslash characters starting at an odd index location from sequences starting at even index location. A sequence of characters that starts at an odd (resp. even) index location and ends at an odd (resp. even) index location must have an even length, and it is therefore a sequence of escaped backslashes. Otherwise, the sequence contains an odd number of backslashes and any quote character following it must be considered escaped.

With the backslash and quote characters identified, we can locate the unescaped quote characters efficiently. We compute a shift followed by a bitwise ANDNOT, eliminating the escaped quote characters.

However, we are interested in finding the location between quotes (the strings), so we can find the actual structural characters. The desired bit pattern would be 1 if there are an odd-numbered number of unescaped quotes at or before our location and zero otherwise.

For example, given the word `0b100010000` representing quote locations (with 1-bit), we wish to compute `0b011110000`. We can achieve this result using the prefix sum of the XOR operation over our bit vector representing unescaped quotes. That is, the resulting bit value at index **i** is the XOR of all bit values up to and including the bit value at index **i** in the input. We can compute such a prefix sum in C++ with a loop that repeatedly apply the bitwise XOR on a left-shifted word:

```cpp
for (i = 0; i < 64; i++) {
    mask = mask xor (mask << 1);
}
```

This prefix sum can be more efficiently implemented as one instruction by using the carry-less multiplication [16] (implemented with the `pclmulqdq` instruction) of our unescaped quote bit vector by another 64-bit word made entirely of ones. The carry-less multiplication works like the regular integer multiplication, but, as the name suggests, without a carry because it relies on the XOR operation instead of the addition. Let us use the convention that given a 64-bit integer **a**, `aᵢ` is the value of the iᵗʰ bit so that `a = Σᵢ₌₀³⁹ aᵢ 2ⁱ`. The regular product between two 64-bit integers **a**, **b** is given by `Σᵢ₌₀³⁹ aᵢ b 2ⁱ` where `aᵢ b 2ⁱ` is zero when `aᵢ` is zero, and otherwise it is **b** left shifted by **i** bits. With these conventions, the carry-less product is given by `⊕₍ᵢ₌₀³⁹ aᵢ b 2ⁱ`; that is, we replace the sum (Σ) by a series of XOR (symbolized by ⊕). Thus we see that when `aᵢ = 1` for all indexes **i**, we get `⊕₍ᵢ₌₀³⁹ b 2ⁱ` which is the prefix sum of the XOR operation. The carry-less multiplication is broadly supported and fast on recent processors due to its applications in cryptography. On skylake processors, the carry-less multiplication (`pclmulqdq`) has a latency of 7 cycles and one can be issued per cycle [11].

---

#### 3.1.2 Vectorized Classification

Mison does one SIMD comparison per character (';', '\', '"', '{', '}'). We proceed similarly to identify the quotes and the backslash characters. However, there are six structural characters, and, for purposes of further analysis, we also need to discover the four permissible white-space characters. Doing ten comparisons and accompanying bitwise OR operations would be expensive. Instead of a comparison, we use the AVX2 `vpshufb` instruction to acts as a vectorized table lookup to do a vectorized classification [21]. The `vpshufb` instruction uses the least significant 4 bits of each byte (low nibble) as an index into a 16-byte table. Other processor architectures (ARM and POWER) have similar SIMD instructions.

By doing one lookup, followed by a 4-bit right shift and a second lookup (using a different table), we can separate the characters into one of two categories: structural characters and white-space characters. The first lookup maps the low nibbles (least significant 4 bits) of each byte to a byte value; the second lookup maps the high nibble (most significant 4 bits) of each byte to a byte value. The two byte values are combined with a bitwise AND.

To see how this can be used to identify sets of characters, suppose that we want to identify the byte values `0x09`, `0x0a` and `0x0d`. The low nibbles are 9, a and d, and the high nibbles are all zeroes. In the first 16-byte lookup table, we set the fourth least significant bit to 1 for the values corresponding to indexes 9, a and d, and only for these three values. In the second 16-byte lookup table, set the fourth least significant bit to 1 for the value at index 0, and only for this value. Then we have that whenever the input values `0x09`, `0x0a` and `0x0d` are encountered, and only for these values, the fourth least significant bit of the result of the bitwise AND is 1. Hence, using two `vpshufb` instructions, a shift and a few bitwise logical operations, we can identify a set of characters. If we could only identify one set of characters, this approach would not be necessarily advantageous, but we can identify many different sets with the same two `vpshufb` instructions. We can repeat the same strategy with new sets of input values, always making them match a given bit index (the fourth in our example). To avoid misclassifications, we need to ensure that each set of input values corresponding to a bit index is uniquely characterized by a set of low nibbles and high nibbles.

We break the set of code-point values corresponding to structural characters into three sets: `{0x2c}`, `{0x3a}`, `{0x5b, 0x5d, 0x7b, 0x7d}`. We match them to the first three bit indexes. We break the set of code-point values corresponding to white-space characters into two sets `{0x09, 0x0a, 0x0d}`, and `{0x20}`. We match them to the fourth and fifth bit indexes. These sets are all uniquely characterized by their low and high nibbles.

See Table 1. The table for the low nibbles is `16, 0, 0, 0, 0, 0, 0, 0, 0, 8, 10, 4, 1, 12, 0, 0`; and the table for the high nibbles is `8, 0, 17, 2, 0, 4, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0`. Applying our algorithm, we get the following results:

| code points desired value |    |
| ------------------------- | -- |
| 0x2c                      | 1  |
| 0x3a                      | 2  |
| 0x5b, 0x5d, 0x7b, 0x7d    | 4  |
| 0x09, 0x0a, 0x0d          | 8  |
| 0x20                      | 16 |
| others                    | 0  |

**Table 1:** Table describing the vectorized classification of the code points. The first column and first row are indexes corresponding to the high and low nibbles. The second column and the second row are the looked up table values. The main table values are the bitwise AND result of the two table values (e.g., 10 AND 8 is 8). The omitted values are zeroes. On the right, we give the desired classification.

We can recognize the structural characters (',', ':', '[', ']', '{', '}') by computing a bitwise AND with `0b111` and the white-space characters with a bitwise AND with `0b11000`. That is, with only two `vpshufb` instructions and a few logical instructions, we can classify all code-point values into one of three sets: structural (comma, colon, braces, brackets), ASCII white-space ('\r', '\n', '\t', ' ') and others. No branching is required.

---

#### 3.1.3 Identification of White-Space and Pseudo-Structural Characters

We also make use of our ability to quickly detect white space in this early stage. We can use another bitset-based transformation to discover locations in our data that follow a structural character or quote followed by zero or more characters of white space; excluding locations within strings, and the structural characters we have already discovered, these locations are the only place that we can expect to see the starts of the JSON atoms (such as numbers whether or not starting with a minus sign, null, true, and false). These locations are thus treated as structural and we term them pseudo-structural characters. Formally, we define pseudo-structural characters as non-white-space characters that are

1. outside quotes and
2. have a predecessor that is either a white-space character or a structural character.

We use a feature of JSON: the legal atoms can all be distinguished from each other by their first character: 't' for true, 'f' for false, 'n' for null and the character class [0-9-] for numerical values.

As a side-effect, identifying pseudo-structural characters helps validate documents. For example, only some ASCII white-space characters are allowed unescaped outside a quoted range in JSON. An isolated disallowed character would be flagged as a pseudo-structural character and subsequently rejected in stage 2. Furthermore, dangling atoms are automatically identified (as the a in `[12 a]`) and will be similarly rejected.

---

#### 3.1.4 Index Extraction

During stage 1, we process blocks of 64 input bytes. The end product is a 64-bit bitset with the bits corresponding to a structural or pseudo-structural characters set to 1. We choose to transform these bitsets into indexes. That is, we seek a list of the locations of the 1-bits. Once we are done with the extraction of the indexes, we can discard the bitset.

Our implementation involves a transformation of bitsets to indexes by use of the count trailing zeroes operation (via the `tzcnt` instruction) and an operation to clear the lowest set bit: `s = s & (s - 1)` in C which compiles to a single instruction (`blsr`). This strategy introduces an unpredictable branch; unless there is a regular pattern in our bitsets, we would expect to have at least one branch miss for each word. However, we employ a technique whereby we extract 8 indexes from our bitset unconditionally, then ignore any indexes that were extracted excessively by means of overwriting those indexes with the next iteration of the index extraction loop. This means that as long as the frequency of our set bits is below 8 bits out of 64 we expect few unpredictable branches.

```cpp
// we decode the set bits from 's'
// to array 'b'
uint64_t s = ...
uint32_t * b = ...
// popcnt instruction
uint32_t cnt = popcount(s);
uint32_t next_base = b + cnt;
while (s) {
    // tzcnt instruction
    *b++ = idx + trailingzeroes(s);
    // blsr instruction
    s = s & (s - 1);
    *b++ = idx + trailingzeroes(s);
    s = s & (s - 1);
    *b++ = idx + trailingzeroes(s);
    s = s & (s - 1);
    *b++ = idx + trailingzeroes(s);
    s = s & (s - 1);
    *b++ = idx + trailingzeroes(s);
    s = s & (s - 1);
    *b++ = idx + trailingzeroes(s);
    s = s & (s - 1);
}
b = next_base;
```

---

#### 3.1.5 Character-Encoding Validation

In our experience, JSON documents are served using the unicode format UTF-8. Some programming languages like Java use UTF-16 for in-memory strings. Yet if we consider the popular JSON Java library Jackson, then the common way to serialize an object to JSON is to call the function `ObjectMapper.writeValue`, and the result is in UTF-8. Indeed, the JSON specification indicates that many implementations do not support encodings other than UTF-8. Parsers like Mison assume that the character encoding is ASCII [17]. Though it is reasonable, a safer assumption is that unicode (UTF8) is used. Not all sequences of bytes are valid UTF-8 and thus a validating parser needs to ensure that the character encoding is correct. We assume that the incoming data is meant to follow UTF-8, and that the parser should produce UTF-8 strings.

UTF-8 is an ASCII superset. The ASCII characters can be represented using a single byte, as a numerical value called code point between 0 and 127 inclusively. UTF-8 extends these 128 code points to a total of 1,114,112 code points. Non-ASCII code points are represented using from two to four bytes, each with the most significant bit set to one. Non-ASCII code points cannot contain ASCII characters: we can therefore remove from an UTF-8 stream of bytes any number of ASCII characters without affecting its validation.

Outside of strings in JSON, all characters must be ASCII. Only the strings require potentially expensive validation. However, there may be many small strings in a document, so it is unclear whether vectorized unicode validation would be beneficial at the individual string level. Thus we validate the input bytes as a whole.

We first test if a block of 64 bytes is made entirely of ASCII characters. It suffices to verify that the most significant bit of all bytes is zero. This optimization might trigger some unpredictable branches, but given how frequently JSON documents might be almost entirely composed of ASCII characters, it is a necessary risk.

If there are non-ASCII characters, we apply a vectorized UTF-8 validation algorithm. It involves several steps, but each one is efficient. We work exclusively with SIMD instructions.

- We need to verify that all byte values are no larger than `0xF4` (or 244): we can achieve this check with an 8-bit saturated subtraction with `0xF4`. The result of the subtraction is zero if and only if the value is no larger than `0xF4`.
- When the byte value `0xED` is found, the next byte must be no larger than `0x9F`; when the byte value `0xF4` is found, the next byte must be no larger than `0x8F`. We can check these conditions with vectorized byte comparisons and byte shifts.
- The byte values `0xC0` and `0xC1` are forbidden. When the byte value is `0xE0`, the next byte value is larger than `0xA0`. When the byte value is `0xF0`, the next byte value is at least `0x90`.
- When a byte value is outside the range of ASCII values, it belongs to one out of four classes, depending on the value of its high nibble.

We use the `vpshufb` instruction to quickly map bytes to one of these categories using values 0, 2, 3, and 4. We map ASCII characters to the value 1. If the value 4 is found (corresponding to a nibble value of f), it should be followed by three values 0. Given such a vector of integers, we can check that it matches a valid sequence of code points by shifting and adding results using saturated subtraction.

All these checks are done using SIMD registers solely, without branching.

---

### 3.2 Stage 2: Building the Tape

In the final stage, we iterate through the indexes found in the first stage. To handle objects and arrays that can be nested, we use a goto-based state machine. Our state is recorded as a stack indicating whether we are in an array or an object. Values such as true, false, null are handled as simple string comparisons. We parse numbers and strings using dedicated functions.

#### 3.2.1 Number Parsing

It is difficult to do number parsing and validation without proceeding in a standard character-by-character manner. We must check for all of the rules of the specification [4]. Thus we proceed as do most parsers. However, we found it useful to test for the common case where there are at least eight digits as part of the fractional portion of the number. Given the eight characters interpreted as a 64-bit integer `val`, we can check whether it is made of eight digits with an inexpensive comparison:

```cpp
(((val & 0xF0F0F0F0F0F0F0F0)
| (((val + 0x0606060606060606)
    & 0xF0F0F0F0F0F0F0F0) >> 4))
        == 0x3333333333333333)
```

When this check is successful, we invoke a fast vectorized function to compute the equivalent integer value. This fast function begins by subtracting from all character values the code point of the character '0', using the `_mm_sub_epi8` intrinsic. Because the digits have consecutive code points in ASCII, this ensures that digit characters are mapped to their values: '0' becomes 0, '1' becomes 1 and so forth. We then invoke the `_mm_maddubs_epi16` intrinsic to multiply every other digit by 10 and add the result to the previous digit, as a 16-bit sum. We repeat a similar process with the `_mm_madd_epi16` intrinsic, this time multiplying every other value by 100 and adding it to the previous value as a 32-bit sum.

The maximal value of these sums is 9999 which fits in a 16-bit integer. We apply the `_mm_packus_epi32` intrinsic to pack the four 32-bit integers into four 16-bit integers. Finally, we call the `_mm_madd_epi16` intrinsic again to multiply every other 16-bit value by 10000 and add it to the preceding value, generating a 32-bit sum.

```cpp
uint32_t parse_eight_digits_unrolled(char *chars) {
    __m128i ascii0 = _mm_set1_epi8('0');
    __m128i mul_1_10 =
        _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1);
    __m128i mul_1_100 = _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1);
    __m128i mul_1_10000 =
        _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1);
    __m128i in = _mm_sub_epi8(_mm_loadu_si128((__m128i *)chars), ascii0);
    __m128i t1 = _mm_maddubs_epi16(in, mul_1_10);
    __m128i t2 = _mm_madd_epi16(t1, mul_1_100);
    __m128i t3 = _mm_packus_epi32(t2, t2);
    __m128i t4 = _mm_madd_epi16(t3, mul_1_10000);
    return _mm_cvtsi128_si32(t4);
}
```

#### 3.2.2 String Validation and Normalization

When encountering a quote character, we always read 32 bytes in a vector register, then look for the quote and the escape characters. If an escape character is found before the first quote character, we use a conventional code path to process the escaped character, otherwise we just write the 32-byte register to our string buffer.

Our string buffer is made of a 32-bit integer indicating the length of the string followed by the string content in UTF-8. As part of the string validation, we must check that no code-point value less than `0x20` is found: we use vectorized comparison.

---

## 4 Experiments

We validate our results through a set of reproducible experiments over varied data. [4.3] reports that the running time during parsing is split evenly between our two stages. [4.4] shows that we use half as many instructions during parsing as our best competitor. [4.5] shows that this reduced instruction count translates into a comparable runtime advantage.

### 4.1 Hardware and Software

Most recent Intel processors are based on the Skylake microarchitecture. We also include a computer with the more recent Cannon Lake microarchitecture in our tests. We summarize the characteristics of our hardware platforms in Table 3.

| Processor      | Base Frequency | Max. Frequency | Microarchitecture       | Memory             | Compiler |
| -------------- | -------------- | -------------- | ----------------------- | ------------------ | -------- |
| Intel i7-6700  | 3.4 GHz        | 3.7 GHz        | Skylake (x64, 2015)     | DDR4 (2133 MT/s)   | GCC 9.1  |
| Intel i3-8121U | 2.2 GHz        | 3.2 GHz        | Cannon Lake (x64, 2018) | LPDDR4 (3200 MT/s) | GCC 9.1  |

**Table 3:** Hardware

Our experiments assume that the JSON document is in memory; we omit disk and network accesses. Popular disks (e.g., NVMe) have a bandwidth of 3 GB/s [30] and more. In practice, JSON documents are frequently ingested from the network. Yet current networking standards allow for speeds exceeding 10 GB/s [8].

After reviewing several parsers, we selected RapidJSON and sajson, two open-source C++ parsers, as references (see Table 4). Palkar et al. describe RapidJSON as the fastest traditional state-machine-based parser available.

| Processor | snapshot            | link                                  |
| --------- | ------------------- | ------------------------------------- |
| simdjson  | June 12th 2019      | https://github.com/lemire/simdjson     |
| RapidJSON | version 1.1.0       | https://github.com/Tencent/rapidjson   |
| sajson    | September 20th 2018 | https://github.com/chadaustin/sajson    |

**Table 4:** Competitive parsers

RapidJSON can either normalize strings in a new buffer or within the input bytes (insitu). We find that the parsing speed is greater in insitu mode, so we present these better numbers. In contrast, sajson only supports insitu parsing. All three parsers do UTF-8 validation of the input. However, the sajson parser does partial UTF-8 validation.

We consider other open-source parsers but we find that they are either slower than RapidJSON, or that they failed to abide by the JSON specification. For example, parsers like gason, jsmn and ultrajson accept `[0e+]` as valid JSON. Parsers like fastjson and ultrajson accept unescaped line breaks in strings.

RapidJSON has compile-time options to enable optimized code paths making use of SIMD optimizations. However, we found both of these compile-time macros (`RAPIDJSON SSE2` and `RAPIDJSON SSE42`) to be systematically detrimental to performance in our tests.

### 4.2 Datasets

Parsing speed is necessarily dependent on the content of the JSON document. For a fair assessment, we chose a wide range of documents. See Table 5 for detailed statistics concerning the chosen files. In Table 6, we present the number of bytes of both the original document and the minified version.

| file           | integer | float  | string | non-ascii | object | array | null | true | false | struct. | byte/struc. |
| -------------- | ------- | ------ | ------ | --------- | ------ | ----- | ---- | ---- | ----- | ------- | ----------- |
| apache_builds  | 2       | 0      | 5289   | 0         | 884    | 3     | 0    | 2    | 1     | 12365   | 10.3        |
| canada         | 46      | 111080 | 12     | 0         | 4      | 56045 | 0    | 0    | 0     | 334374  | 6.7         |
| citm_catalog   | 14392   | 0      | 26604  | 348       | 10937  | 10451 | 1263 | 0    | 0     | 135991  | 12.7        |
| github_events  | 149     | 0      | 1891   | 4         | 180    | 19    | 24   | 57   | 7     | 4657    | 14.0        |
| gsoc-2018      | 0       | 0      | 34128  | 0         | 3793   | 0     | 0    | 0    | 0     | 75842   | 43.9        |
| instruments    | 4935    | 0      | 6889   | 0         | 1012   | 194   | 431  | 17   | 109   | 27174   | 8.1         |
| marine_ik      | 130225  | 114950 | 38268  | 0         | 9680   | 28377 | 0    | 6    | 0     | 643013  | 4.6         |
| mesh           | 40613   | 32400  | 11     | 0         | 3      | 3610  | 0    | 0    | 0     | 153275  | 4.7         |
| mesh.pretty    | 40613   | 32400  | 11     | 0         | 3      | 3610  | 0    | 0    | 0     | 153275  | 10.3        |
| numbers        | 0       | 10001  | 0      | 0         | 0      | 1     | 0    | 0    | 0     | 20004   | 7.5         |
| random         | 5002    | 0      | 33005  | 103482    | 4001   | 1001  | 0    | 495  | 505   | 88018   | 5.8         |
| twitterescaped | 2108    | 1      | 18099  | 0         | 1264   | 1050  | 1946 | 345  | 2446  | 55264   | 10.1        |
| twitter        | 2108    | 1      | 18099  | 95406     | 1264   | 1050  | 1946 | 345  | 2446  | 55264   | 11.4        |
| update-center  | 0       | 0      | 27229  | 49        | 1896   | 1937  | 0    | 134  | 252   | 63420   | 8.4         |

**Table 5:** Datasets statistics.

| file           | bytes (minified) | bytes (original) | ratio |
| -------------- | ---------------- | ---------------- | ----- |
| apache_builds  | 94653            | 127275           | 74%   |
| canada         | 2251027          | 2251027          | 100%  |
| citm_catalog   | 500299           | 1727204          | 29%   |
| github_events  | 53329            | 65132            | 82%   |
| gsoc-2018      | 3073766          | 3327831          | 92%   |
| instruments    | 108313           | 220346           | 49%   |
| marine_ik      | 1834197          | 2983466          | 61%   |
| mesh           | 650573           | 723597           | 90%   |
| mesh.pretty    | 753399           | 1577353          | 48%   |
| numbers        | 150121           | 150124           | 100%  |
| random         | 461466           | 510476           | 90%   |
| twitterescaped | 562408           | 562408           | 100%  |
| twitter        | 466906           | 631514           | 74%   |
| update-center  | 533177           | 533178           | 100%  |

**Table 6:** Datasets sizes.

---

### 4.3 Running Time Distribution

We present the distribution of cycles per stage for each test file:

- **1: no utf8:** refers to the time spent in stage 1, except for UTF-8 validation. This time is between 0.5 and 1 cycles per input byte.
- **1: just utf8:** refers to the time spent doing UTF-8 validation. It is negligible for all but the random and twitter documents.
- **2: core:** is for the time spent in stage 2 except for string and number parsing.
- **2: numbers:** refers to the time spent in number parsing in stage 2. Roughly a third of the CPU cycles are spent parsing numbers in canada, marine_ik, mesh, mesh.pretty and numbers.
- **2: strings:** refers to the time spent parsing strings in stage 2.

About half the CPU cycles per input byte (between 0.5 and 3 cycles) are spent in stage 1. The running time of the parser depends on the characteristics of the JSON document. For a high accuracy (R² ≥ 0.99), we have the following cost models:

- Total running time (Skylake): `19 × F + 11 × S + 0.92 × B`
- Total running time (Cannon Lake): `17 × F + 11 × S + 0.88 × B`

where **F** = number of floating-point numbers, **S** = structural elements, and **B** = bytes.

---

### 4.4 Fewer Instructions

The main benefit of SIMD instructions is to do more work with fewer instructions. On average, simdjson uses about half as many instructions as sajson and RapidJSON.

| file               | simdjson | RapidJSON | sajson   | RapidJSON/simdjson | sajson/simdjson |
| ------------------ | -------- | --------- | -------- | ------------------ | --------------- |
| apache_builds      | 5.6      | 15.9      | 10.0     | 2.8                | 1.8             |
| canada             | 12.9     | 26.2      | 20.9     | 2.0                | 1.6             |
| citm_catalog       | 5.3      | 11.7      | 11.1     | 2.2                | 2.1             |
| github_events      | 4.9      | 15.5      | 10.1     | 3.2                | 2.1             |
| gsoc-2018          | 3.2      | 15.0      | 11.2     | 4.7                | 3.5             |
| instruments        | 6.4      | 15.3      | 12.6     | 2.4                | 2.0             |
| marine_ik          | 13.4     | 23.7      | 20.6     | 1.8                | 1.5             |
| mesh.pretty        | 9.0      | 17.0      | 14.9     | 1.9                | 1.7             |
| mesh               | 14.3     | 27.2      | 23.3     | 1.9                | 1.6             |
| numbers            | 11.7     | 25.9      | 18.8     | 2.2                | 1.6             |
| random             | 8.9      | 19.6      | 15.4     | 2.2                | 1.7             |
| twitter            | 5.5      | 14.3      | 11.5     | 2.6                | 2.1             |
| twitterescaped     | 9.3      | 16.5      | 13.7     | 1.8                | 1.5             |
| update-center      | 6.2      | 18.4      | 12.1     | 3.0                | 2.0             |
| **average**        | **8.3**  | **18.7**  | **14.7** |                    |                 |
| **geometric mean** |          |           |          | **2.4**            | **1.9**         |

**Table 8:** Instructions per byte required to parse and validate documents.

The simdjson parser uses many more vector instructions (see Table 7).

| Instruction | description                         | latency | throughput |
| ----------- | ----------------------------------- | ------- | ---------- |
| vpaddb      | Add 8-bit integers                  | 1       | 0.5        |
| vpalignr    | Concatenate pairs of 16-byte blocks | 1       | 1          |
| vpand       | Compute the bitwise AND             | 1       | 0.5        |
| vpcmpeqb    | Compare 8-bit integers (=)          | 1       | 0.5        |
| vpcmpgtb    | Compare 8-bit integers (>)          | 1       | 0.5        |
| vperm2i128  | Shuffle integers                    | 3       | 1          |
| vpmaxub     | Compute max of 8-bit integers       | 1       | 0.5        |
| vpor        | Compute the bitwise OR              | 1       | 0.5        |
| vpshufb     | Shuffle bytes                       | 1       | 1          |
| vpsrld      | Right shift 32-bit integers         | 1       | 1          |
| vpsrlw      | Right shift 16-bit integers         | 1       | 1          |
| vpsubusb    | Subtract 8-bit integers             | 1       | 0.5        |
| vptest      | Test for zero                       | 3       | 1          |

**Table 7:** 256-bit SIMD instructions on Skylake processors [11].

---

### 4.5 Speed Comparison

We present raw parsing speeds in Table 9. On our Skylake (3.4 GHz) processor, our parser (simdjson) achieves and even surpasses 2 GB/s in six instances.

| file                  | apache_builds | canada | citm_catalog | github_events | gsoc-2018 | instruments | marine_ik | mesh | mesh.pretty | numbers | random | twitter | twitterescaped | update-center |
| --------------------- | ------------- | ------ | ------------ | ------------- | --------- | ----------- | --------- | ---- | ----------- | ------- | ------ | ------- | -------------- | ------------- |
| **Skylake (3.4 GHz)** |               |        |              |               |           |             |           |      |             |         |        |         |                |               |
| simdjson              | 2.3           | 1.1    | 2.5          | 2.5           | 3.2       | 2.1         | 0.94      | 0.95 | 1.5         | 1.1     | 1.4    | 2.2     | 1.2            | 1.9           |
| RapidJSON             | 0.48          | 0.43   | 0.86         | 0.46          | 0.51      | 0.59        | 0.45      | 0.41 | 0.67        | 0.45    | 0.38   | 0.45    | 0.38           | 0.38          |
| sajson                | 0.80          | 0.43   | 1.0          | 0.79          | 1.0       | 0.76        | 0.45      | 0.41 | 0.67        | 0.45    | 0.58   | 0.72    | 0.62           | 0.64          |

**Table 9:** Parsing throughput (GB/s).

Our parser is again twice as fast as the reference parsers for tree traversal. We achieve 1.8 GB/s to parse and scan the resulting tree on the twitter document.
