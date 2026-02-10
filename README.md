# ghanta

This repository contains tooling to download and subset NOAA AORC v1.1 precipitation (`apcp`) from the public AWS Open Data Zarr dataset.

## Files

- `scripts/download_aorc.py`: downloads/subsets AORC data, computes area-weighted mean precipitation, performs QA checks, and writes outputs.
- `.github/workflows/aorc_download.yml`: manual GitHub Actions workflow that runs the script and uploads artifacts.

## Running locally

```bash
python -m pip install --upgrade pip
pip install xarray s3fs pandas dask netcdf4 numpy
python scripts/download_aorc.py
```

Outputs are written to `outputs/`:

- `aorc_subset_apcp_2010_2020.nc`
- `aorc_area_mean_apcp_hourly_2010_2020.csv`

## Running in GitHub Actions

1. Go to **Actions**.
2. Select **Download AORC**.
3. Click **Run workflow**.
4. Download the `aorc-outputs` artifact.

> If the default AORC URI changes, set the `AORC_ZARR_URI` environment variable.
