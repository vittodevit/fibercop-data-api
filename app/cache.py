from datetime import datetime
from typing import List, Dict, Optional
from collections import Counter
import threading


class DataCache:
    def __init__(self):
        self._lock = threading.Lock()
        self.latest_date: Optional[datetime] = None
        self.parsed_data: List[Dict] = []
        self.raw_csv_bytes: bytes = b""
        self.original_filename: str = ""
        self.last_fetch_time: Optional[datetime] = None
        self.fetch_status: str = "none"
        self.last_error: Optional[str] = None
        self.list_data: List[Dict] = []
        self.listmap_data: List[List] = []
        self.details_map: Dict[int, Dict] = {}
        self.stats_data: Dict = {}

    def update(
        self,
        latest_date: datetime,
        parsed_data: List[Dict],
        raw_csv_bytes: bytes,
        original_filename: str,
    ):
        with self._lock:
            self.latest_date = latest_date
            self.parsed_data = parsed_data
            self.raw_csv_bytes = raw_csv_bytes
            self.original_filename = original_filename
            self.last_fetch_time = datetime.now()
            self.fetch_status = "success"
            self.last_error = None
        self.build_derived_data(parsed_data)

    def set_error(self, error: str):
        with self._lock:
            self.last_fetch_time = datetime.now()
            self.fetch_status = "error"
            self.last_error = error

    def set_fetching(self):
        with self._lock:
            self.fetch_status = "fetching"

    def build_derived_data(self, parsed_data: List[Dict]):
        list_data = []
        listmap_data = []
        details_map = {}

        province_counter = Counter()
        tipo_counter = Counter()
        stato_counter = Counter()
        availability_years = Counter()

        for idx, record in enumerate(parsed_data):
            tipo = record.get("TIPO", "")
            tipo_code = 0 if tipo == "CRO" else 1

            list_data.append(
                {
                    "id": idx,
                    "lat": float(record.get("LATITUDINE", 0)) or 0,
                    "lon": float(record.get("LONGITUDINE", 0)) or 0,
                    "type": tipo,
                }
            )

            listmap_data.append(
                [
                    idx,
                    float(record.get("LATITUDINE", 0)) or 0,
                    float(record.get("LONGITUDINE", 0)) or 0,
                    tipo_code,
                ]
            )

            details_map[idx] = record

            prov = record.get("PROVINCIA", "")
            if prov:
                province_counter[prov] += 1

            if tipo:
                tipo_counter[tipo] += 1

            stato = record.get("STATO", "")
            if stato:
                stato_counter[stato] += 1

            avail_date = record.get("DATA_DISPONIBILITA", "")
            if avail_date and len(avail_date) >= 4:
                availability_years[avail_date[:4]] += 1

        top_provinces = dict(province_counter.most_common(20))
        other_count = sum(
            count
            for prov, count in province_counter.items()
            if prov not in top_provinces
        )
        if other_count > 0:
            top_provinces["OTHER"] = other_count

        with self._lock:
            self.list_data = list_data
            self.listmap_data = listmap_data
            self.details_map = details_map
            self.stats_data = {
                "total": len(parsed_data),
                "by_provincia": top_provinces,
                "by_tipo": dict(tipo_counter),
                "by_stato": dict(stato_counter),
                "by_availability_year": dict(availability_years),
                "updated_at": datetime.now().isoformat(),
            }

    def get_data(self) -> Dict:
        with self._lock:
            return {
                "latest_update_date": self.latest_date.isoformat()
                if self.latest_date
                else None,
                "last_fetch_time": self.last_fetch_time.isoformat()
                if self.last_fetch_time
                else None,
                "fetch_status": self.fetch_status,
                "data": self.parsed_data,
            }

    def get_csv(self) -> tuple[bytes, str]:
        with self._lock:
            return self.raw_csv_bytes, self.original_filename

    def get_list_data(self) -> List[Dict]:
        with self._lock:
            return self.list_data

    def get_listmap_data(self) -> List[List]:
        with self._lock:
            return self.listmap_data

    def get_detail_by_id(self, id: int) -> Optional[Dict]:
        with self._lock:
            return self.details_map.get(id)

    def get_stats(self) -> Dict:
        with self._lock:
            return self.stats_data


cache = DataCache()
