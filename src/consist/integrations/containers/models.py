from typing import Dict, List, Optional, Any
from pydantic import BaseModel


class ContainerDefinition(BaseModel):
    """
    Represents the 'Configuration' of a container run for hashing purposes.
    """

    image: str
    image_digest: Optional[str] = None  # Specific SHA for reproducibility
    command: List[str]
    environment: Dict[str, str]
    backend: str

    # Extra args that might affect execution (e.g. resource limits)
    extra_args: Dict[str, Any] = {}

    def to_hashable_config(self) -> Dict[str, Any]:
        """Returns a clean dictionary for Consist canonical hashing."""
        return self.model_dump(exclude_none=True)
