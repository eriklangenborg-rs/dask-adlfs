Dask interface to Azure-Datalake Storage
----------------------------------------

Warning: this code is experimental and untested.

To install the backend, do ``import dask_adlfs``, which will allow for URLs starting with ``"adl://"`` in
dask functions.

Usage Notes:
------------

From [StackOverflow](https://stackoverflow.com/questions/47741801/dask-how-to-read-csv-files-into-a-dataframe-from-microsoft-azure-blob)

```python
import dask.dataframe as dd
df = dd.read_csv('adl://mystore/path/to/*.csv', storage_options={
    tenant_id='mytenant', client_id='myclient', 
    client_secret='mysecret'})
```
