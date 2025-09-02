from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class AssetTags:
    layer: str
    source: str
    domain: str
    sharable: Optional[bool] = None

    def to_dict(self) -> Dict[str, str]:
        tags = {
            "source": self.source,
            "domain": self.domain,
            "layer": self.layer,
            "sharable": str(self.sharable) if self.sharable is not None else None
        }
        return {k: v for k, v in tags.items() if v is not None}