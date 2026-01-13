SEXP savvy_hello__ffi(void);
SEXP savvy_int_times_int__ffi(SEXP c_arg__x, SEXP c_arg__y);
SEXP savvy_read_sas__ffi(SEXP c_arg__path);
SEXP savvy_sas_column_names__ffi(SEXP c_arg__path);
SEXP savvy_sas_metadata_json__ffi(SEXP c_arg__path);
SEXP savvy_sas_row_count__ffi(SEXP c_arg__path);
SEXP savvy_to_upper__ffi(SEXP c_arg__x);
SEXP savvy_write_sas__ffi(SEXP c_arg__path, SEXP c_arg__sink, SEXP c_arg__output);

// methods and associated functions for Person
SEXP savvy_Person_associated_function__ffi(void);
SEXP savvy_Person_name__ffi(SEXP self__);
SEXP savvy_Person_new__ffi(void);
SEXP savvy_Person_set_name__ffi(SEXP self__, SEXP c_arg__name);
