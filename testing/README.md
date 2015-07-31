## Test comparator

Tester implements a quick differ for CSV files produced by ```sys-file-index -single``` or by the included database query.

Used to double check the validity of the generated output.

Tester exists at the first difference found.

### Usage

```
$ testing/testing sys_file_output.csv database_output.csv
```
