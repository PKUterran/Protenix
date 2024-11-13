import os
import gzip
import lmdb
import tqdm
from typing import Tuple
from multiprocessing import Pool

ENCODING = "utf-8"

input_dir: str = "/vepfs/fs_projects/yangsw/af3-dev/release_data/mmcif"
output_lmdb_path: str = "/vepfs/fs_projects/yangsw/af3-dev/release_data/lmdb/mmcif.lmdb"
n_workers: int = 32
n_commit: int = 1000

env_new = lmdb.open(
    output_lmdb_path,
    subdir=False,
    readonly=False,
    lock=False,
    readahead=False,
    meminit=False,
    max_readers=126,
    map_size=100 * 1024 * 1024 * 1024,
)

print(f"Checking processed mmCIFs in LMDB {output_lmdb_path}:")
todo_cif_names, finished_cif_names = [], []
with env_new.begin() as txn:
    with txn.cursor() as cursor:
        def check_processed(_cif_name: str) -> Tuple[str, bool]:
            return _cif_name, cursor.set_key(bytes(_cif_name, ENCODING))

        cif_names = [f[:4] for f in os.listdir(input_dir)]
        with Pool(n_workers) as pool:
            for ret in tqdm.tqdm(pool.imap_unordered(check_processed, cif_names), total=len(cif_names), ncols=60):
                cif_name, is_processed = ret
                if is_processed:
                    finished_cif_names.append(cif_name)
                else:
                    todo_cif_names.append(cif_name)

print(f"\tTODO {len(todo_cif_names)}; FINISHED {len(finished_cif_names)}")
print("Processing:")

def process_one(_cif_name: str) -> Tuple[bytes, bytes, bool]:
    try:
        with open(os.path.join(input_dir, f"{_cif_name}.cif")) as _fp:
            cif_lines = _fp.read()
        return bytes(_cif_name, ENCODING), gzip.compress(cif_lines.encode(ENCODING)), True
    except Exception as e:
        print(f"{_cif_name} encounters {e.__class__.__name__}({e})\n")
        return bytes(_cif_name, ENCODING), b"", False

txn_write = env_new.begin(write=True)
i = 0
with Pool(n_workers) as pool:
    for ret in tqdm.tqdm(pool.imap_unordered(process_one, todo_cif_names), total=len(todo_cif_names), ncols=60):
        cif_name_bytes, cif_gz_bytes, succeed = ret
        if succeed:
            txn_write.put(cif_name_bytes, cif_gz_bytes)
        i += 1
        if i % n_commit == 0:
            txn_write.commit()
            txn_write = env_new.begin(write=True)

txn_write.commit()
env_new.close()
