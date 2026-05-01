from abc import ABC, abstractmethod

class SchemaProvisioner(ABC):
    @abstractmethod
    def provision_tenant(self, tenant: str) -> None:
        """create new schema on the database."""
        pass