# CLAUDE.md

## Project Overview

This is an R-based research project. All analysis should be structured as a
reproducible pipeline using the `targets` package.

## Project Structure

```
_targets.R        # Main pipeline entry point — sources target lists from targets/
targets/          # Modular target list files (each returns a list of tar_target objects)
core/             # Utility functions called by targets (grouped by topic, e.g., census_data_utils.R)
data-raw/         # Raw, unmodified source data (never overwrite)
test_utils/       # Test/validation functions (grouped by topic, e.g., census_data_tests.R)
tests/            # Target lists that call test functions from test_utils/
```

## Pipeline Architecture

### targets package

- All analysis must go through the `targets` pipeline. Do not write standalone
  scripts that run outside the pipeline.
- The `_targets.R` file at the project root should source and combine target
  lists from the `targets/` directory. Keep `_targets.R` minimal — it should
  mostly call `tar_source()` and combine lists.
- Each file in `targets/` should define and return a `list()` of
  `tar_target()` objects. Group related targets into the same file
  (e.g., `targets/census_data.R`, `targets/models.R`).
- All functions that targets call should live in `core/`. Group related
  utility functions together in the same file. Name files with a `_utils.R`
  suffix using `snake_case`
  (e.g., `core/census_data_utils.R`, `core/spatial_utils.R`).

### Target storage formats

- **Default:** Save targets as **parquet** files (`format = "parquet"`).
- **Spatial data:** If a target has a geometry column (i.e., it is an `sf`
  object), save it as `format = "rds"`.
- **Non-tabular objects:** If a target is not a `data.table`, `data.frame`, or
  `tibble` (e.g., model objects, lists, plots), save it as `format = "rds"`.
- **External files:** For targets that produce or track files on disk, use
  `format = "file"`.

## R Coding Conventions

### Package loading

Use `box::use()` inside every function to load only the packages and functions
needed. Do not use `library()` calls in function files. Combine all imports
into a single `box::use()` call. Example:

```r
f_clean_census <- function(raw_dt) {
  box::use(data.table[...], magrittr[`%>%`])

  raw_dt %>%
    .[year >= 1990] %>%
    .[, .(state, county, population)]
}
```

### Piping

Use the **magrittr pipe** (`%>%`), not the base R pipe (`|>`). The magrittr
pipe is more flexible, especially with `data.table`'s bracket syntax
(e.g., `dt %>% .[, .(col1, col2)]`).

### Data manipulation

Use **`data.table`** for all data manipulation and analysis.

### Column selection

Use `CLmisc::select_by_ref()` to select columns from a `data.table` by
reference. This modifies the table in place and is preferred over subsetting
when you want to drop columns.

### Style

- Use `snake_case` for all variable names, function names, and file names.
- Name functions descriptively after what they do, prefixed with `f_` to
  distinguish project-local functions from package functions
  (e.g., `f_clean_census_data`, `f_estimate_did_model`).
- Minimal code comments. Only add high-level comments when the intent is not
  obvious from the code itself. Let clear naming be the documentation.

### Mutating data.tables inside functions

When a function receives a `data.table` as an argument and needs to modify it
by reference (e.g., with `:=`), **always `copy()` the input first**. The
`targets` package caches objects, so modifying a passed-in `data.table` by
reference will silently corrupt the cached upstream target.

```r
f_transform_census <- function(raw_dt) {
  box::use(data.table[...], magrittr[`%>%`])

  dt <- copy(raw_dt)
  dt[, pop_thousands := population / 1000]
  dt
}
```

## CLmisc Package (Personal Utility Package)

The `CLmisc` package provides helper functions used throughout the project.
Key functions for data acquisition:

| Function                   | Purpose                                          |
|----------------------------|--------------------------------------------------|
| `CLmisc::get_rnhgis_shp()` | Downloads and saves shapefiles from NHGIS        |
| `CLmisc::get_rnhgis_tst()` | Downloads and saves time series tables from NHGIS |
| `CLmisc::get_rnhgis_ds()`  | Downloads and saves datasets from NHGIS          |
| `CLmisc::select_by_ref()`  | Selects columns from a data.table by reference   |

When working with NHGIS data, prefer these functions over manual downloads.
Check function documentation with `?CLmisc::<function>` if you need to
understand the arguments.

## Common Workflow

A typical task is converting a raw or exploratory analysis (often from
`scratch/`) into a reproducible `targets` pipeline. When doing this:

1. Break the analysis into discrete, cacheable steps — each step becomes a
   target.
2. Extract the logic into `f_`-prefixed functions in the appropriate
   `core/*_utils.R` file.
3. Define the corresponding `tar_target()` calls in a `targets/*.R` file.
4. Add sanity checks: write check functions in `test_utils/` and define
   corresponding targets in `tests/`.
5. Use `copy()` on any `data.table` inputs that will be modified by reference.

## Sanity Checks and Validation

After creating, transforming, or merging datasets, suggest appropriate sanity
checks. Do not silently assume the output is correct. Common checks to
recommend:

### Data integrity
- **Row counts:** Flag unexpected changes in row counts after joins or filters.
  Compare before and after.
- **Key uniqueness:** Verify that identifier columns (e.g., FIPS codes,
  state-year pairs) are unique when they should be.
- **Missing values:** Check for unexpected `NA`s in key columns after joins
  or transformations.
- **Value ranges:** Verify that numeric columns are within plausible bounds
  (e.g., percentages between 0–100, populations non-negative).
- **Duplicate detection:** After merges, check for unintended row duplication.

### Joins and merges
- Report match rates — how many rows from each side matched?
- Flag unmatched observations and suggest investigating them.

### Estimation and modeling
- **Coefficient signs:** Do estimated signs match theoretical expectations?
- **Magnitude checks:** Are coefficients in a plausible range?
- **Sample sizes:** Report the effective sample size used in estimation.
- **Convergence:** For iterative estimators, confirm convergence.

### General practice
- When a check would be informative, write it as a function in `test_utils/`
  and include it as a target in `tests/` (so checks are reproducible and
  cached alongside the pipeline).
- Use `stopifnot()` for hard constraints that should halt the pipeline if
  violated.
- Use `message()` or `warning()` for soft checks that inform but don't block.
