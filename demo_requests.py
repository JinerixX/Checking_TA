import pandas as pd
from tqdm import tqdm
import asyncio
import json
import httpx
from coroexecutor import CoroutineExecutor
from asyncstdlib import itertools as aioitertools
from pydantic import BaseModel, Field, ConfigDict


class ProductContent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    gtin: str
    manufacturng_date: str = Field(alias="manufacturingDate")
    expiry_date: str = Field(alias="expiryDate")


class Product(BaseModel):
    model_config = ConfigDict(extra="ignore")

    epc: str
    parent_epc: str | None = Field(alias="parentEpc", default=None)
    content: list[ProductContent] = Field(default_factory=list)


class KvintaClient:
    base_url = "https://public-api.prod.bakalar.kvinta.cloud"


    def __init__(self, api_key, timeout, retries=3):
        self._api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                'Authorization': api_key
            },
            event_hooks={"response": [self._raise_for_status]},
            timeout=timeout,
            transport=httpx.AsyncHTTPTransport(
                retries=retries
            )
        )


    async def get_product(self, epc):
        payload = {
            "epc": epc,
            "listChildren": False,
            "format": "NON_BRACKETED"
        }

        response = await self._client.post(url="/epcis/v1/getEpcHierInfo", json=payload)
        return Product(**response.json())


    @staticmethod
    async def _raise_for_status(response):
        response.raise_for_status()


async def main(
    api_key, 
    request_workers,  
    request_timeout,
    output_file,
    output_excel_file,
    input_url = None,
    input_file = None
):
    client = KvintaClient(api_key, request_timeout)

    epc_list, epc_len = None, None
    if input_url is not None:
        file_response = httpx.get(input_url)
        epc_list = file_response.content.decode().split("\n")
        epc_len = len(epc_list)

    elif input_file is not None:
        with open(input_file, 'r') as f:
            epc_list = f.readlines()
            epc_len = len(epc_list)

    else:
        raise ValueError("one of 'input_file' or 'input_url' must be specified")

    async def product_generator(epc_generator, request_workers, callback=None):
        async with CoroutineExecutor(max_workers=request_workers) as exe:
            async for product in exe.map(client.get_product, epc_generator):
                yield product
                if callback is not None:
                    callback()

    async def group_products_by_parent_epc(products):
        async for parent_epc, products in aioitertools.groupby(products, lambda x: x.parent_epc):
            yield parent_epc, (product.epc async for product in products)

    # iter 1

    with tqdm(initial=0, total=epc_len, desc="Items") as progress:
        items_parent_groups = {
            parent_epc: [product_epc async for product_epc in product_epcs]
            async for parent_epc, product_epcs in group_products_by_parent_epc(
                product_generator(
                    epc_list, 
                    request_workers, 
                    callback=lambda: progress.update(1)
                )
            )
        }

    # iter 2

    with tqdm(initial=0, total=len(items_parent_groups.keys()), desc="Items Parents") as progress:
        parent_top_parent_groups = group_products_by_parent_epc(
            product_generator(
                items_parent_groups.keys(), 
                request_workers, 
                callback=lambda: progress.update(1)
            )
        )

    # write result

    result = []
    async for top_parent_epc, parent_epcs in parent_top_parent_groups:
        top_parent = await client.get_product(top_parent_epc)

        result.append({
            "epc": top_parent_epc,
            **top_parent.model_dump()["content"][0],
            "children": [
                {
                    "box_epc": parent_epc,
                    "children": items_parent_groups[parent_epc]
                }
                async for parent_epc in parent_epcs
            ]
        })

    with open(output_file, "w") as file:
        json.dump(result, fp=file)

    # write result to excel

    df = pd.json_normalize(
        result, 
        record_path=['children', 'children'], 
        meta=['epc', 'gtin', 'manufacturng_date', 'expiry_date', ['children', 'box_epc']],
    )
    df.columns = [
        "Code",
        "Pallet SSCC",
        "GTIN",
        "Mfg Date",
        "Expiry Date",
        "Case SSCC"
    ]
    df = df[[
        "Code",
        "GTIN",
        "Mfg Date",
        "Expiry Date",
        "Case SSCC",
        "Pallet SSCC"
    ]]
    df.to_excel(output_excel_file, sheet_name="Sheet1", index=False)

    print("Done")


if __name__ == "__main__":
    asyncio.run(main(
        api_key='Basic ',
        request_workers=128,
        request_timeout=300,
        #input_file='epc_File.txt',
        input_url="https://t24310213.p.clickup-attachments.com/t24310213/5a272a1c-b5f7-4517-8e99-3a6195b8814b/%D0%9F%D0%B8%D0%B2%D0%BE%20%D1%84%D0%B8%D0%BB%D1%8C%D1%82%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D0%B5%20%D0%BF%D0%B0%D1%81%D1%82%D0%B5%D1%80%D0%B8%D0%B7%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D0%B5%20%D1%81%D0%B2%D0%B5%D1%82%D0%BB%D0%BE%D0%B5%20%D0%B1%D0%B5%D0%B7%D0%B0%D0%BB%D0%BA%D0%BE%D0%B3%D0%BE%D0%BB%D1%8C%D0%BD%D0%BE%D0%B5%20%C2%ABBakalar%20nealkoholicky%20za%20studena%20chmeleny%C2%BB%20(%D0%91%D0%B0%D0%BA%D0%B0%D0%BB%D0%B0%D1%80%20%D0%B1%D0%B5%D0%B7%D0%B0%D0%BB%D0%BA%D0%BE%D0%B3%D0%BE%D0%BB%D1%8C%D0%BD%D0%BE%D0%B5%20%D1%85%D0%BE%D0%BB%D0%BE%D0%B4%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%BE%D1%85%D0%BC%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F)_%D0%9E%D1%82%D0%B3%D1%80%D1%83%D0%B7%D0%BA%D0%B0%20%D0%BE%D1%82%20RAK%20(%20KVINTA%20)%20%23%20NS.desadv.0722.24_gtin_08594053493183_qty_37200.txt",
        output_file='result.json',
        output_excel_file='result.xlsx',
    ))