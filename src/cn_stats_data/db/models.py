from datetime import datetime
from enum import Enum
import json
from typing import List, Optional

from cn_stats_util.models import Metric, Region, HistoricalData

__all__ = ["MetricCode", "RegionCode", "MetricHistoricalData", "ProcessData", "RegionCodeDownloadCheckpoint", "MetricCodeDownloadCheckpoint"]


class MetricCode(Metric):
    def __init__(
        self,
        db_code: str,
        code: str,
        name: str,
        explanation: str,
        is_parent: bool,
        memo: Optional[str] = None,
        unit: Optional[str] = None,
        parent: Optional["MetricCode"] = None,
        children: Optional[list["MetricCode"]] = None,
        is_deleted: Optional[bool] = False,
        created_time: Optional[datetime] = None,
        last_updated_time: Optional[datetime] = None,
        **kwargs,
    ):
        super().__init__(
            db_code,
            code,
            name,
            explanation,
            is_parent,
            memo,
            unit,
            parent,
            children,
            **kwargs,
        )
        self.is_deleted = is_deleted
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        if is_parent and children is None:
            self._is_parent = is_parent

    def __eq__(self, other):
        if not isinstance(other, Metric):
            return False
        return (
            super().__eq__(other)
            and self.name == other.name
            and self.explanation == other.explanation
            and self.memo == other.memo
            and self.unit == other.unit
            and self.is_deleted == (other.is_deleted if isinstance(other, MetricCode) else False)
            and self.extra_attributes == other.extra_attributes
        )

    def __repr__(self) -> str:
        base_repr = super().__repr__()
        return f"{base_repr[:-1]}, is_deleted={self.is_deleted}, created_time={self.created_time}, last_updated_time={self.last_updated_time})"


class RegionCode(Region):
    def __init__(
        self,
        db_code: str,
        code: str,
        name: str,
        explanation: str,
        is_parent: bool,
        parent: Optional["Region"] = None,
        children: Optional[List["Region"]] = None,
        is_deleted: Optional[bool] = False,
        created_time: Optional[datetime] = None,
        last_updated_time: Optional[datetime] = None,
        **kwargs,
    ):
        super().__init__(
            db_code,
            code,
            name,
            explanation,
            is_parent,
            parent,
            children,
            **kwargs,
        )
        self.is_deleted = is_deleted
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        if is_parent and children is None:
            self._is_parent = is_parent

    def __eq__(self, other):
        if not isinstance(other, Region):
            return False
        return (
            super().__eq__(other)
            and self.name == other.name
            and self.explanation == other.explanation
            and self.is_deleted == (other.is_deleted if isinstance(other, RegionCode) else False)
            and self.extra_attributes == other.extra_attributes
        )

    def __repr__(self) -> str:
        base_repr = super().__repr__()
        extra_info = f", is_deleted={self.is_deleted}, created_time={self.created_time}, last_updated_time={self.last_updated_time}"
        return base_repr[:-1] + extra_info + ")"


class MetricHistoricalData(HistoricalData):
    def __init__(
        self,
        metric_code: str,
        db_code: str,
        region_code: Optional[str],
        period: str,
        data: float,
        has_data: bool,
        is_deleted: Optional[bool] = False,
        created_time: Optional[datetime] = None,
        last_updated_time: Optional[datetime] = None,
        **kwargs,
    ):
        super().__init__(
            metric_code=metric_code,
            db_code=db_code,
            region_code=region_code,
            period=period,
            data=data,
            has_data=has_data,
            **kwargs,
        )
        self.is_deleted = is_deleted
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def __eq__(self, other):
        if not isinstance(other, MetricHistoricalData):
            return False
        return (
            super().__eq__(other)
            and self.is_deleted == other.is_deleted
        )

    def __repr__(self) -> str:
        base_repr = super().__repr__()
        return f"{base_repr[:-1]}, is_deleted={self.is_deleted}, created_time={self.created_time}, last_updated_time={self.last_updated_time})"
    

class ProcessData:
    def __init__(self, process_id: str, data: str):
        self.process_id = process_id
        self.data = data

class ProcessStatus(Enum):
    PENDING = ('Pending')
    RUNNING = ('Running')
    DONE = ('Done')
    FAILED = ('Failed')

    def __init__(self, status: str):
        self.status = status        

class MetricCodeDownloadCheckpoint:
    def __init__(
            self,
            db_code: Optional[str],
            metric_code: Optional[str],
            db_checkpoint: Optional[str] = None, 
            metric_checkpoint: Optional[List[str]] = None,
            status: ProcessStatus = ProcessStatus.PENDING):
        self.db_code = db_code
        self.metric_code = metric_code
        self.db_checkpoint = db_checkpoint
        self.metric_checkpoint = metric_checkpoint if metric_checkpoint else []
        self._db_checkpoint_located = False
        self._metric_checkpoint_located = False
        self.status: ProcessStatus = status

    def finish(self) -> None:
        self._db_checkpoint_located = False
        self._metric_checkpoint_located = False
        self.db_checkpoint = None
        self.metric_checkpoint = None
        self.status = ProcessStatus.DONE

    def reset_checkpoint(self) -> None:
        self.db_code = None
        self.metric_code = None
        self.db_checkpoint = None
        self.metric_checkpoint = []
        self._db_checkpoint_located = False
        self._metric_checkpoint_located = False

    def reset_if_parameters_changed(self, db_code: Optional[str], metric_code: Optional[str]) -> None:
        if (self.status != ProcessStatus.RUNNING 
            or (self.db_code if self.db_code else "") != (db_code if db_code else "") 
            or (self.metric_code if self.metric_code else "") != (metric_code if metric_code else "")):        
            self.reset_checkpoint()
            self.db_code = db_code
            self.metric_code = metric_code
        self.status = ProcessStatus.RUNNING
    
    def _set_db_checkpoint(self, db_code: str) -> None:
        self.db_checkpoint = db_code
        self.metric_checkpoint = []

    def need_skip_db(self, db_code: str) -> bool:
        if not self.db_checkpoint or self._db_checkpoint_located:
            self._set_db_checkpoint(db_code=db_code)
            self._db_checkpoint_located = True
            return False
        if db_code.__eq__(self.db_checkpoint):
            self._db_checkpoint_located = True
            return False
        else:
            return True
        
    def need_skip_metric(self, metric: Metric) -> bool:
        
        path: List[str] = []
        m: Metric = metric
        while m is not None:
            path.append(m.code)
            m = m.parent
        path.reverse()

        if not self.metric_checkpoint or self._metric_checkpoint_located:
            self.metric_checkpoint = path
            self._metric_checkpoint_located = True
            return False
        
        len_cp = len(self.metric_checkpoint)
        len_pa = len(path)

        if len_cp < len_pa:
            return True
        
        sub_cp = self.metric_checkpoint[0:len_pa] if len_pa < len_cp else self.metric_checkpoint
        is_equal = sub_cp == path

        if len_cp == len_pa:
            self._metric_checkpoint_located = True
            
        return not is_equal    
    
    def to_dict(self) -> dict:
        return {
            'db_code': self.db_code,
            'metric_code': self.metric_code,
            'db_checkpoint': self.db_checkpoint,
            'metric_checkpoint': self.metric_checkpoint,
            'status': self.status.status
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MetricCodeDownloadCheckpoint":
        instance = cls(
            db_code=data.get('db_code'),
            metric_code=data.get('metric_code')
        )
        instance.db_checkpoint = data.get('db_checkpoint', None)
        instance.metric_checkpoint = data.get('metric_checkpoint', [])
        instance.status = ProcessStatus(data.get('status', ProcessStatus.PENDING.status))
        return instance
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
        
    @classmethod
    def from_json(cls, json_str: str) -> "MetricCodeDownloadCheckpoint":
        return cls.from_dict(json.loads(json_str))
    

class RegionCodeDownloadCheckpoint:
    """
    Checkpoint for downloading region codes
    """
    def __init__(
            self, 
            db_code: Optional[str] = None, 
            region_code: Optional[str] = None,
            db_checkpoint: Optional[str] = None, 
            region_checkpoint: Optional[List[str]] = None,
            status: ProcessStatus = ProcessStatus.PENDING):
        self.db_code = db_code
        self.region_code = region_code
        self.db_checkpoint = db_checkpoint
        self.region_checkpoint = region_checkpoint
        self._db_checkpoint_located = False
        self._region_checkpoint_located = False
        self.status = status

    def reset_if_parameters_changed(self, db_code: Optional[str], region_code: Optional[str]) -> None:
        if (self.status != ProcessStatus.RUNNING 
            or self.db_code != db_code or self.region_code != region_code):
            self.reset_checkpoint()
            self.db_code = db_code
            self.region_code = region_code
        self.status = ProcessStatus.RUNNING

    def need_skip_db(self, db_code: str) -> bool:
        if not self.db_checkpoint or self._db_checkpoint_located:
            self._set_db_checkpoint(db_code=db_code)
            self._db_checkpoint_located = True
            return False
        if db_code.__eq__(self.db_checkpoint):
            self._db_checkpoint_located = True
            return False
        else:
            return True

    def need_skip_region(self, region: Region) -> bool:
        path: List[str] = []
        r: Region = region
        while r is not None:
            path.append(r.code)
            r = r.parent
        path.reverse()

        if not self.region_checkpoint or self._region_checkpoint_located:
            self.region_checkpoint = path
            self._region_checkpoint_located = True
            return False

        len_cp = len(self.region_checkpoint)
        len_pa = len(path)
        if len_cp < len_pa:
            return True

        sub_cp = self.region_checkpoint[0:len_pa] if len_pa < len_cp else self.region_checkpoint
        is_equal = sub_cp == path

        if len_cp == len_pa:
            self._region_checkpoint_located = True

        return not is_equal

    def reset_checkpoint(self) -> None:
        self.db_code = None
        self.region_code = None
        self._db_checkpoint_located = False
        self._region_checkpoint_located = False
        self.db_checkpoint = None
        self.region_checkpoint = None
        self.status = ProcessStatus.PENDING

    def finish(self) -> None:
        self._db_checkpoint_located = False
        self._region_checkpoint_located = False
        self.db_checkpoint = None
        self.region_checkpoint = None
        self.status = ProcessStatus.DONE

    def _set_db_checkpoint(self, db_code: str) -> None:
        self.db_checkpoint = db_code

    def to_dict(self) -> dict:
        return {
            'db_code': self.db_code,
            'region_code': self.region_code,
            'db_checkpoint': self.db_checkpoint,
            'region_checkpoint': self.region_checkpoint,
            'status': self.status.value
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RegionCodeDownloadCheckpoint":
        instance = cls(
            db_code=data.get('db_code'),
            region_code=data.get('region_code')
        )
        instance.db_checkpoint = data.get('db_checkpoint')
        instance.region_checkpoint = data.get('region_checkpoint')
        instance.status = ProcessStatus(data.get('status', ProcessStatus.PENDING.value))
        return instance
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
        
    @classmethod
    def from_json(cls, json_str: str) -> "RegionCodeDownloadCheckpoint":
        return cls.from_dict(json.loads(json_str))