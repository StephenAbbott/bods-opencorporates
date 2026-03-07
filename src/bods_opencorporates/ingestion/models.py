"""Source data models for OpenCorporates data.

These dataclasses represent the canonical internal representation of
OpenCorporates data before it is transformed into BODS format.
They are populated by both the API client and the CSV reader.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OCAddress:
    """An address as represented in OpenCorporates data."""

    street_address: str | None = None
    locality: str | None = None
    region: str | None = None
    postal_code: str | None = None
    country: str | None = None

    @property
    def full_address(self) -> str | None:
        """Combine address components into a single string."""
        parts = [
            p
            for p in [self.street_address, self.locality, self.region, self.postal_code]
            if p and p.strip()
        ]
        return ", ".join(parts) if parts else None

    @classmethod
    def from_api_dict(cls, data: dict | None) -> OCAddress | None:
        """Create from an OpenCorporates API address object."""
        if not data:
            return None
        return cls(
            street_address=data.get("street_address"),
            locality=data.get("locality"),
            region=data.get("region"),
            postal_code=data.get("postal_code"),
            country=data.get("country"),
        )

    @classmethod
    def from_csv_row(
        cls,
        row: dict,
        prefix: str = "registered_address.",
    ) -> OCAddress | None:
        """Create from a CSV row with prefixed address columns."""
        street = row.get(f"{prefix}street_address") or row.get("registered_address_street_address")
        locality = row.get(f"{prefix}locality") or row.get("registered_address_locality")
        region = row.get(f"{prefix}region") or row.get("registered_address_region")
        postal_code = row.get(f"{prefix}postal_code") or row.get("registered_address_postal_code")
        country = row.get(f"{prefix}country") or row.get("registered_address_country")

        if any([street, locality, region, postal_code, country]):
            return cls(
                street_address=street or None,
                locality=locality or None,
                region=region or None,
                postal_code=postal_code or None,
                country=country or None,
            )
        return None


@dataclass
class OCCompany:
    """A company as represented in OpenCorporates data."""

    company_number: str
    jurisdiction_code: str
    name: str
    company_type: str | None = None
    incorporation_date: str | None = None
    dissolution_date: str | None = None
    current_status: str | None = None
    registered_address: OCAddress | None = None
    previous_names: list[str] = field(default_factory=list)
    branch: str | None = None
    home_jurisdiction_code: str | None = None
    home_company_number: str | None = None
    industry_codes: list[str] = field(default_factory=list)
    native_company_number: str | None = None
    retrieved_at: str | None = None

    @classmethod
    def from_api_dict(cls, data: dict) -> OCCompany:
        """Create from an OpenCorporates API company object.

        Expected structure: data["company"] from the API response.
        """
        company = data.get("company", data)
        previous = company.get("previous_names", [])
        prev_names = []
        if isinstance(previous, list):
            for pn in previous:
                if isinstance(pn, dict):
                    prev_names.append(pn.get("company_name", ""))
                elif isinstance(pn, str):
                    prev_names.append(pn)

        return cls(
            company_number=str(company["company_number"]),
            jurisdiction_code=company["jurisdiction_code"],
            name=company["name"],
            company_type=company.get("company_type"),
            incorporation_date=company.get("incorporation_date"),
            dissolution_date=company.get("dissolution_date"),
            current_status=company.get("current_status"),
            registered_address=OCAddress.from_api_dict(
                company.get("registered_address")
            ),
            previous_names=prev_names,
            branch=company.get("branch"),
            home_jurisdiction_code=company.get("home_jurisdiction_code"),
            home_company_number=company.get("home_jurisdiction_company_number"),
            industry_codes=[
                ic.get("uid", "") for ic in (company.get("industry_codes") or [])
            ],
            native_company_number=company.get("native_company_number"),
            retrieved_at=company.get("retrieved_at"),
        )

    @classmethod
    def from_csv_row(cls, row: dict) -> OCCompany:
        """Create from a CSV DictReader row."""
        previous = row.get("previous_names", "")
        prev_names = [n.strip() for n in previous.split("|") if n.strip()] if previous else []

        return cls(
            company_number=str(row["company_number"]),
            jurisdiction_code=row["jurisdiction_code"],
            name=row["name"],
            company_type=row.get("company_type") or None,
            incorporation_date=row.get("incorporation_date") or None,
            dissolution_date=row.get("dissolution_date") or None,
            current_status=row.get("current_status") or None,
            registered_address=OCAddress.from_csv_row(row),
            previous_names=prev_names,
            branch=row.get("branch") or None,
            home_jurisdiction_code=row.get("home_jurisdiction_code") or None,
            home_company_number=row.get("home_jurisdiction_company_number") or None,
            retrieved_at=row.get("retrieved_at") or None,
        )


@dataclass
class OCOfficer:
    """An officer/director as represented in OpenCorporates data."""

    id: str
    company_number: str
    jurisdiction_code: str
    full_name: str
    position: str
    first_name: str | None = None
    last_name: str | None = None
    title: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    nationality: str | None = None
    country_of_residence: str | None = None
    partial_date_of_birth: str | None = None
    occupation: str | None = None
    officer_type: str | None = None  # "Person", "Company", or None
    address: OCAddress | None = None
    person_uid: str | None = None
    current_status: str | None = None
    retrieved_at: str | None = None

    @classmethod
    def from_api_dict(cls, data: dict, jurisdiction_code: str, company_number: str) -> OCOfficer:
        """Create from an OpenCorporates API officer object.

        Expected structure: data["officer"] from the API response.
        """
        officer = data.get("officer", data)

        # Extract officer ID from the OC ID or link
        officer_id = str(officer.get("id", ""))
        if not officer_id:
            # Try to extract from opencorporates_url
            url = officer.get("opencorporates_url", "")
            if url:
                officer_id = url.rstrip("/").split("/")[-1]

        address_data = officer.get("address")
        address = None
        if address_data:
            if isinstance(address_data, dict):
                address = OCAddress.from_api_dict(address_data)
            elif isinstance(address_data, str):
                address = OCAddress(street_address=address_data)

        return cls(
            id=officer_id,
            company_number=company_number,
            jurisdiction_code=jurisdiction_code,
            full_name=officer.get("name", ""),
            position=officer.get("position", officer.get("role", "")),
            first_name=officer.get("first_name"),
            last_name=officer.get("last_name"),
            title=officer.get("title"),
            start_date=officer.get("start_date"),
            end_date=officer.get("end_date"),
            nationality=officer.get("nationality"),
            country_of_residence=officer.get("country_of_residence"),
            partial_date_of_birth=officer.get("date_of_birth"),
            occupation=officer.get("occupation"),
            officer_type=officer.get("type"),  # "Person" or "Company"
            address=address,
            person_uid=officer.get("person_uid"),
            current_status=officer.get("current_status"),
            retrieved_at=officer.get("retrieved_at"),
        )

    @classmethod
    def from_csv_row(cls, row: dict) -> OCOfficer:
        """Create from a CSV DictReader row."""
        address = None
        addr_str = row.get("address.in_full") or row.get("address_in_full")
        if addr_str:
            address = OCAddress(
                street_address=row.get("address.street_address") or row.get("address_street_address"),
                locality=row.get("address.locality") or row.get("address_locality"),
                region=row.get("address.region") or row.get("address_region"),
                postal_code=row.get("address.postal_code") or row.get("address_postal_code"),
                country=row.get("address.country") or row.get("address_country"),
            )
            if not any([address.street_address, address.locality, address.region]):
                address = OCAddress(street_address=addr_str)

        return cls(
            id=str(row.get("id", row.get("uid", ""))),
            company_number=str(row["company_number"]),
            jurisdiction_code=row["jurisdiction_code"],
            full_name=row.get("full_name") or row.get("name", ""),
            position=row.get("position") or row.get("role", ""),
            first_name=row.get("first_name") or None,
            last_name=row.get("last_name") or None,
            title=row.get("title") or None,
            start_date=row.get("start_date") or None,
            end_date=row.get("end_date") or None,
            nationality=row.get("nationality") or None,
            country_of_residence=row.get("country_of_residence") or None,
            partial_date_of_birth=row.get("partial_date_of_birth") or row.get("date_of_birth") or None,
            occupation=row.get("occupation") or None,
            officer_type=row.get("type") or None,
            address=address,
            person_uid=row.get("person_uid") or None,
            current_status=row.get("current_status") or None,
            retrieved_at=row.get("retrieved_at") or None,
        )


@dataclass
class OCRelationship:
    """A corporate relationship as represented in OpenCorporates data.

    Covers: control_statement, subsidiary, branch, share_parcel.
    """

    relationship_type: str
    subject_company_number: str
    subject_jurisdiction_code: str
    subject_name: str
    object_company_number: str
    object_jurisdiction_code: str
    object_name: str
    percentage_min_share_ownership: float | None = None
    percentage_max_share_ownership: float | None = None
    percentage_min_voting_rights: float | None = None
    percentage_max_voting_rights: float | None = None
    number_of_shares: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    retrieved_at: str | None = None

    @classmethod
    def from_csv_row(cls, row: dict) -> OCRelationship:
        """Create from a CSV DictReader row from the relationships file."""

        def parse_float(val: str | None) -> float | None:
            if not val or not val.strip():
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def parse_int(val: str | None) -> int | None:
            if not val or not val.strip():
                return None
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        return cls(
            relationship_type=row.get("relationship_type", ""),
            subject_company_number=str(row.get("subject.company_number", row.get("subject_company_number", ""))),
            subject_jurisdiction_code=row.get("subject.jurisdiction_code", row.get("subject_jurisdiction_code", "")),
            subject_name=row.get("subject.name", row.get("subject_name", "")),
            object_company_number=str(row.get("object.company_number", row.get("object_company_number", ""))),
            object_jurisdiction_code=row.get("object.jurisdiction_code", row.get("object_jurisdiction_code", "")),
            object_name=row.get("object.name", row.get("object_name", "")),
            percentage_min_share_ownership=parse_float(
                row.get("percentage_min_share_ownership")
            ),
            percentage_max_share_ownership=parse_float(
                row.get("percentage_max_share_ownership")
            ),
            percentage_min_voting_rights=parse_float(
                row.get("percentage_min_voting_rights")
            ),
            percentage_max_voting_rights=parse_float(
                row.get("percentage_max_voting_rights")
            ),
            number_of_shares=parse_int(row.get("number_of_shares")),
            start_date=row.get("start_date") or None,
            end_date=row.get("end_date") or None,
            retrieved_at=row.get("retrieved_at") or row.get("updated_at") or None,
        )
