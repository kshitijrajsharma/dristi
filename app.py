import os
import time
from typing import Dict, Union

import aiohttp
import dask.array as da
import geopandas
import rasterio
from dask.distributed import Client
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from geojson_pydantic import Feature
from pydantic import BaseModel, Field
from rasterio.windows import from_bounds

STATIC_DIR = os.getenv("STATIC_DIR", "static")
os.makedirs(STATIC_DIR, exist_ok=True)

app = FastAPI(app="Dristi")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

client = Client()


class GeoJSONInput(BaseModel):
    geom: Union[Feature] = Field(
        example={
            "type": "Feature",
            "properties": {},
            "geometry": {
                "coordinates": [
                    [
                        [83.74180309481727, 28.281889569333856],
                        [83.74180309481727, 28.281320507276973],
                        [83.74249860235204, 28.281320507276973],
                        [83.74249860235204, 28.281889569333856],
                        [83.74180309481727, 28.281889569333856],
                    ]
                ],
                "type": "Polygon",
            },
        }
    )


async def download_cog(cog_url: str) -> str:
    cog_file_name = os.path.basename(cog_url)
    file_path = os.path.join(STATIC_DIR, cog_file_name)

    if os.path.exists(file_path):
        return file_path

    async with aiohttp.ClientSession() as session:
        async with session.get(cog_url) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=500, detail=f"Failed to download COG from {cog_url}"
                )
            with open(file_path, "wb") as tmp_file:
                tmp_file.write(await response.read())
                return file_path


async def compute_raster_statistics(cog_url: str, geojson: Dict) -> Dict[str, float]:
    compute_start = time.time()
    try:
        gdf = geopandas.GeoDataFrame.from_features(
            features=[geojson], crs=4326
        )  ## hard coded only support 4326

        cog_path = await download_cog(cog_url)

        with rasterio.open(cog_path) as src:
            gdf = gdf.to_crs(
                epsg=32644  ## hard coded
            )  ## Temp FIX : Please follow standard geograhic transformation if crs mispatch
            minx, miny, maxx, maxy = gdf.total_bounds
            print(minx, miny, maxx, maxy)
            window = from_bounds(minx, miny, maxx, maxy, src.transform)
            out_image = da.from_array(src.read(1, window=window), chunks=(1024, 1024))
            print(out_image)
            mean = out_image.mean().compute() if out_image.size > 0 else None
            print(f"Computation time : {time.time()-compute_start} s")
            return {
                "mean": float(mean) if mean is not None else None,
            }
    except Exception as e:
        raise e
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/compute/")
async def compute_statistics(
    geojson: GeoJSONInput,
    cog_url: str = "https://oin-hotosm.s3.us-east-1.amazonaws.com/669a3a711684770001c33a8f/0/669a3a711684770001c33a90.tif",
):
    request_start_time = time.time()
    try:
        statistics = await compute_raster_statistics(cog_url, geojson.geom.dict())
        response_time = time.time() - request_start_time
        return statistics
    except Exception as e:
        raise e
        raise HTTPException(status_code=500, detail=str(e))
