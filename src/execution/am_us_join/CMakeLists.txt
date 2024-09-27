add_library_unity(duckdb_am_us_join OBJECT am_us_join_inner.cpp
                  am_us_join_mark.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_am_us_join>
    PARENT_SCOPE)
