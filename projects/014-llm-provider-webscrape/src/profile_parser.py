"""
Profile Parser for Healthcare Provider Data
DA-173: Provider Profile Web Enrichment POC
"""

import re
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

class ProfileParser:
    """
    Parses raw LLM responses into structured provider profile data.
    Handles JSON extraction, data normalization, and validation.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the parser with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def parse_response(self, raw_response: str) -> Dict[str, Any]:
        """
        Parse the raw LLM response into a structured profile.
        
        Args:
            raw_response: Raw text response from LLM
            
        Returns:
            Structured profile dictionary
        """
        try:
            # Try to extract JSON from the response
            json_data = self._extract_json(raw_response)
            
            if json_data:
                # Validate and normalize the JSON structure
                profile = self._normalize_profile(json_data)
            else:
                # Fall back to text parsing if no JSON found
                profile = self._parse_text_response(raw_response)
            
            # Clean and validate the profile
            profile = self._clean_profile(profile)
            profile = self._validate_profile(profile)
            
            return profile
            
        except Exception as e:
            self.logger.error(f"Error parsing response: {str(e)}")
            return self._get_empty_profile()
    
    def _extract_json(self, text: str) -> Optional[Dict]:
        """
        Extract JSON object from text response.
        
        Args:
            text: Raw text containing JSON
            
        Returns:
            Parsed JSON dictionary or None
        """
        # Try to find JSON block in various formats
        patterns = [
            r'```json\s*(.*?)\s*```',  # Markdown code block
            r'```\s*(.*?)\s*```',       # Generic code block
            r'\{[^{}]*\{.*\}[^{}]*\}',  # Nested JSON object
            r'\{.*\}',                   # Simple JSON object
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.DOTALL)
            if matches:
                # Try to parse each match
                for match in matches:
                    try:
                        # Clean the match
                        if not match.startswith('{'):
                            # Find the JSON part in the match
                            json_start = match.find('{')
                            json_end = match.rfind('}') + 1
                            if json_start >= 0 and json_end > json_start:
                                match = match[json_start:json_end]
                        
                        # Parse JSON
                        data = json.loads(match)
                        
                        # Check if this looks like a profile
                        if self._is_profile_json(data):
                            return data
                            
                    except json.JSONDecodeError:
                        continue
        
        return None
    
    def _is_profile_json(self, data: Dict) -> bool:
        """Check if the JSON looks like a provider profile."""
        # Check for expected top-level keys
        expected_keys = ['name', 'specialty', 'education', 'professionalRoles']
        profile_keys = ['basic_info', 'professional', 'education', 'industry_relationships']
        
        # Check if it has old-style keys
        if any(key in data for key in expected_keys):
            return True
        
        # Check if it has new-style nested structure
        if any(key in data for key in profile_keys):
            return True
        
        # Check if wrapped in a profile key
        if 'profile' in data and isinstance(data['profile'], dict):
            return True
        
        return False
    
    def _normalize_profile(self, json_data: Dict) -> Dict:
        """
        Normalize the JSON structure to our standard format.
        
        Args:
            json_data: Raw JSON from LLM
            
        Returns:
            Normalized profile structure
        """
        # If data is wrapped in 'profile' key, unwrap it
        if 'profile' in json_data and isinstance(json_data['profile'], dict):
            json_data = json_data['profile']
        
        # Initialize the standard structure
        profile = self._get_empty_profile()
        
        # Map old format to new format
        if 'name' in json_data:
            # Old format from original script
            profile = self._map_old_format(json_data)
        elif 'basic_info' in json_data:
            # Already in new format
            profile = json_data
        else:
            # Try to map whatever we have
            profile = self._map_flexible_format(json_data)
        
        return profile
    
    def _map_old_format(self, data: Dict) -> Dict:
        """Map old format fields to new structure."""
        profile = self._get_empty_profile()
        
        # Basic info
        profile['basic_info']['name'] = data.get('name', '')
        profile['basic_info']['npi'] = data.get('npi', '')
        profile['basic_info']['specialty'] = data.get('specialty', '')
        profile['basic_info']['subspecialties'] = data.get('subspecialties', [])
        
        # Professional
        if 'professionalRoles' in data:
            roles = data['professionalRoles']
            if isinstance(roles, dict):
                profile['professional']['current_positions'] = self._parse_positions(roles.get('current', []))
                profile['professional']['previous_positions'] = self._parse_positions(roles.get('previous', []))
        
        if 'hospitalAffiliations' in data:
            profile['professional']['hospital_affiliations'] = self._parse_hospital_affiliations(data['hospitalAffiliations'])
        
        if 'practiceLocations' in data:
            profile['professional']['practice_locations'] = self._parse_practice_locations(data['practiceLocations'])
        
        # Education
        if 'education' in data:
            profile['education'] = self._parse_education(data['education'])
        
        # Industry relationships
        if 'industryCorporateAffiliations' in data:
            affiliations = data['industryCorporateAffiliations']
            if isinstance(affiliations, list):
                profile['industry_relationships'] = self._parse_industry_relationships(affiliations)
        
        # Research
        if 'researchActivities' in data:
            research = data['researchActivities']
            if isinstance(research, dict):
                profile['research']['publications'] = self._parse_publications(research.get('notablePublications', []))
                profile['research']['research_interests'] = research.get('researchInterests', [])
        
        # Leadership
        if 'boardMemberships' in data:
            profile['leadership']['board_memberships'] = self._parse_board_memberships(data['boardMemberships'])
        
        if 'advisoryRoles' in data:
            profile['leadership']['advisory_roles'] = self._parse_advisory_roles(data['advisoryRoles'])
        
        # Professional activities
        if 'speakingEngagements' in data:
            profile['professional_activities']['speaking_engagements'] = data.get('speakingEngagements', [])
        
        if 'professionalMemberships' in data:
            profile['professional_activities']['professional_societies'] = self._parse_societies(data['professionalMemberships'])
        
        if 'awardsHonorsRecognitions' in data:
            profile['professional_activities']['awards_recognitions'] = self._parse_awards(data['awardsHonorsRecognitions'])
        
        return profile
    
    def _map_flexible_format(self, data: Dict) -> Dict:
        """Flexibly map any format to our structure."""
        profile = self._get_empty_profile()
        
        # Try to map common field names
        for key, value in data.items():
            lower_key = key.lower()
            
            # Name variations
            if 'name' in lower_key:
                profile['basic_info']['name'] = str(value)
            elif 'npi' in lower_key:
                profile['basic_info']['npi'] = str(value)
            elif 'specialty' in lower_key or 'specialt' in lower_key:
                if 'sub' in lower_key:
                    profile['basic_info']['subspecialties'] = self._ensure_list(value)
                else:
                    profile['basic_info']['specialty'] = str(value)
            
            # Professional variations
            elif 'position' in lower_key or 'role' in lower_key:
                if 'current' in lower_key:
                    profile['professional']['current_positions'] = self._ensure_list(value)
                elif 'previous' in lower_key or 'past' in lower_key:
                    profile['professional']['previous_positions'] = self._ensure_list(value)
            elif 'hospital' in lower_key or 'affiliation' in lower_key:
                profile['professional']['hospital_affiliations'] = self._ensure_list(value)
            
            # Education variations
            elif 'education' in lower_key or 'school' in lower_key:
                if isinstance(value, dict):
                    profile['education'] = value
                elif isinstance(value, list):
                    profile['education']['medical_school'] = value[0] if value else {}
            
            # Research variations
            elif 'publication' in lower_key or 'research' in lower_key:
                if 'publication' in lower_key:
                    profile['research']['publications'] = self._ensure_list(value)
                else:
                    profile['research']['research_interests'] = self._ensure_list(value)
            
            # Leadership variations
            elif 'board' in lower_key:
                profile['leadership']['board_memberships'] = self._ensure_list(value)
            elif 'advisory' in lower_key:
                profile['leadership']['advisory_roles'] = self._ensure_list(value)
        
        return profile
    
    def _parse_text_response(self, text: str) -> Dict:
        """
        Parse a text response when no JSON is found.
        
        Args:
            text: Raw text response
            
        Returns:
            Structured profile from text parsing
        """
        profile = self._get_empty_profile()
        
        # Extract name
        name_match = re.search(r'(?:Dr\.|Doctor|Prof\.|Professor)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', text)
        if name_match:
            profile['basic_info']['name'] = name_match.group(0).strip()
        
        # Extract NPI
        npi_match = re.search(r'NPI[:\s]+(\d{10})', text, re.IGNORECASE)
        if npi_match:
            profile['basic_info']['npi'] = npi_match.group(1)
        
        # Extract specialty
        specialty_match = re.search(r'(?:specialty|specializes in)[:\s]+([^\n,.]+)', text, re.IGNORECASE)
        if specialty_match:
            profile['basic_info']['specialty'] = specialty_match.group(1).strip()
        
        # Extract positions (look for titles)
        titles = ['Chief', 'Director', 'Chair', 'Head', 'Professor', 'Physician', 'Doctor']
        for title in titles:
            pattern = rf'{title}[^,\n]*(?:at|of|,)[^,\n]+'
            matches = re.findall(pattern, text)
            for match in matches:
                position = self._parse_position_from_text(match)
                if position:
                    profile['professional']['current_positions'].append(position)
        
        # Extract education
        edu_patterns = [
            r'(?:graduated from|attended|MD from|medical school:)\s*([^,\n]+)',
            r'([A-Z][^,\n]*(?:University|College|School of Medicine)[^,\n]*)',
        ]
        for pattern in edu_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if 'medical' in match.lower() or 'md' in match.lower():
                    profile['education']['medical_school'] = {'institution': match.strip()}
                    break
        
        return profile
    
    def _parse_position_from_text(self, text: str) -> Optional[Dict]:
        """Parse a position from text."""
        # Try to split into title and organization
        parts = re.split(r'\s+(?:at|of)\s+', text, maxsplit=1)
        if len(parts) == 2:
            return {
                'title': parts[0].strip(),
                'organization': parts[1].strip()
            }
        return None
    
    def _parse_positions(self, positions: Any) -> List[Dict]:
        """Parse position data into standard format."""
        if not positions:
            return []
        
        positions = self._ensure_list(positions)
        parsed = []
        
        for pos in positions:
            if isinstance(pos, dict):
                parsed.append({
                    'title': pos.get('title', ''),
                    'organization': pos.get('organization', ''),
                    'department': pos.get('department', ''),
                    'start_date': pos.get('start_date', ''),
                    'end_date': pos.get('end_date', ''),
                    'location': pos.get('location', '')
                })
            elif isinstance(pos, str):
                # Try to parse string position
                position = self._parse_position_from_text(pos)
                if position:
                    parsed.append(position)
        
        return parsed
    
    def _parse_hospital_affiliations(self, affiliations: Any) -> List[Dict]:
        """Parse hospital affiliation data."""
        if not affiliations:
            return []
        
        affiliations = self._ensure_list(affiliations)
        parsed = []
        
        for aff in affiliations:
            if isinstance(aff, dict):
                parsed.append({
                    'hospital_name': aff.get('hospital_name', aff.get('name', '')),
                    'affiliation_type': aff.get('affiliation_type', aff.get('type', '')),
                    'department': aff.get('department', ''),
                    'status': aff.get('status', 'Active')
                })
            elif isinstance(aff, str):
                parsed.append({
                    'hospital_name': aff,
                    'affiliation_type': '',
                    'department': '',
                    'status': 'Active'
                })
        
        return parsed
    
    def _parse_practice_locations(self, locations: Any) -> List[Dict]:
        """Parse practice location data."""
        if not locations:
            return []
        
        locations = self._ensure_list(locations)
        parsed = []
        
        for loc in locations:
            if isinstance(loc, dict):
                parsed.append({
                    'practice_name': loc.get('practice_name', loc.get('name', '')),
                    'address': loc.get('address', ''),
                    'city': loc.get('city', ''),
                    'state': loc.get('state', ''),
                    'zip': loc.get('zip', ''),
                    'phone': loc.get('phone', ''),
                    'accepts_new_patients': loc.get('accepts_new_patients', None)
                })
            elif isinstance(loc, str):
                parsed.append({
                    'practice_name': loc,
                    'address': '',
                    'city': '',
                    'state': '',
                    'zip': '',
                    'phone': '',
                    'accepts_new_patients': None
                })
        
        return parsed
    
    def _parse_education(self, education: Any) -> Dict:
        """Parse education data."""
        result = {
            'medical_school': {},
            'residency': [],
            'fellowship': [],
            'certifications': []
        }
        
        if isinstance(education, dict):
            result.update(education)
        elif isinstance(education, list):
            for item in education:
                if isinstance(item, dict):
                    edu_type = item.get('type', '').lower()
                    if 'medical' in edu_type or 'md' in edu_type:
                        result['medical_school'] = item
                    elif 'residency' in edu_type:
                        result['residency'].append(item)
                    elif 'fellowship' in edu_type:
                        result['fellowship'].append(item)
                    elif 'certification' in edu_type or 'board' in edu_type:
                        result['certifications'].append(item)
        
        return result
    
    def _parse_industry_relationships(self, relationships: List) -> Dict:
        """Parse industry relationship data."""
        result = {
            'pharmaceutical': [],
            'medical_device': [],
            'healthcare_startups': [],
            'consulting': []
        }
        
        for rel in relationships:
            if isinstance(rel, dict):
                company = rel.get('company_name', rel.get('company', ''))
                rel_type = rel.get('relationship_type', rel.get('type', '')).lower()
                
                if 'pharma' in company.lower() or 'pharmaceutical' in rel_type:
                    result['pharmaceutical'].append(rel)
                elif 'device' in company.lower() or 'device' in rel_type:
                    result['medical_device'].append(rel)
                elif 'startup' in company.lower() or 'startup' in rel_type:
                    result['healthcare_startups'].append(rel)
                else:
                    result['consulting'].append(rel)
            elif isinstance(rel, str):
                # Simple string relationship
                if 'pharma' in rel.lower():
                    result['pharmaceutical'].append({'company_name': rel, 'relationship_type': ''})
                else:
                    result['consulting'].append({'company_name': rel, 'relationship_type': ''})
        
        return result
    
    def _parse_publications(self, publications: Any) -> List[Dict]:
        """Parse publication data."""
        if not publications:
            return []
        
        publications = self._ensure_list(publications)
        parsed = []
        
        for pub in publications:
            if isinstance(pub, dict):
                parsed.append(pub)
            elif isinstance(pub, str):
                parsed.append({
                    'title': pub,
                    'journal': '',
                    'year': None,
                    'authors': [],
                    'pmid': '',
                    'doi': ''
                })
        
        return parsed
    
    def _parse_board_memberships(self, boards: Any) -> List[Dict]:
        """Parse board membership data."""
        if not boards:
            return []
        
        boards = self._ensure_list(boards)
        parsed = []
        
        for board in boards:
            if isinstance(board, dict):
                parsed.append(board)
            elif isinstance(board, str):
                parsed.append({
                    'organization': board,
                    'position': 'Board Member',
                    'committee': '',
                    'term_start': '',
                    'term_end': ''
                })
        
        return parsed
    
    def _parse_advisory_roles(self, roles: Any) -> List[Dict]:
        """Parse advisory role data."""
        if not roles:
            return []
        
        roles = self._ensure_list(roles)
        parsed = []
        
        for role in roles:
            if isinstance(role, dict):
                parsed.append(role)
            elif isinstance(role, str):
                parsed.append({
                    'organization': role,
                    'role': 'Advisor',
                    'focus_area': '',
                    'start_date': '',
                    'compensation': None
                })
        
        return parsed
    
    def _parse_societies(self, societies: Any) -> List[Dict]:
        """Parse professional society data."""
        if not societies:
            return []
        
        societies = self._ensure_list(societies)
        parsed = []
        
        for society in societies:
            if isinstance(society, dict):
                parsed.append(society)
            elif isinstance(society, str):
                parsed.append({
                    'society_name': society,
                    'membership_type': '',
                    'join_year': None,
                    'leadership_roles': []
                })
        
        return parsed
    
    def _parse_awards(self, awards: Any) -> List[Dict]:
        """Parse awards and recognition data."""
        if not awards:
            return []
        
        awards = self._ensure_list(awards)
        parsed = []
        
        for award in awards:
            if isinstance(award, dict):
                parsed.append(award)
            elif isinstance(award, str):
                # Try to extract year from string
                year_match = re.search(r'(\d{4})', award)
                parsed.append({
                    'award_name': award,
                    'awarding_organization': '',
                    'year': int(year_match.group(1)) if year_match else None,
                    'description': ''
                })
        
        return parsed
    
    def _ensure_list(self, value: Any) -> List:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
    
    def _clean_profile(self, profile: Dict) -> Dict:
        """Clean and standardize the profile data."""
        
        def clean_value(value):
            if isinstance(value, str):
                # Remove excess whitespace
                value = ' '.join(value.split())
                # Remove "None available" type values
                if value.lower() in ['none available', 'none', 'n/a', 'not available']:
                    return ''
                return value
            elif isinstance(value, list):
                return [clean_value(v) for v in value if v]
            elif isinstance(value, dict):
                return {k: clean_value(v) for k, v in value.items()}
            return value
        
        return clean_value(profile)
    
    def _validate_profile(self, profile: Dict) -> Dict:
        """Validate the profile data."""
        
        # Validate NPI if present
        if profile.get('basic_info', {}).get('npi'):
            npi = profile['basic_info']['npi']
            if not self._validate_npi(npi):
                self.logger.warning(f"Invalid NPI detected: {npi}")
                profile['basic_info']['npi'] = ''
        
        # Validate dates
        profile = self._validate_dates(profile)
        
        # Remove empty lists and dicts
        profile = self._remove_empty(profile)
        
        return profile
    
    def _validate_npi(self, npi: str) -> bool:
        """Validate NPI number using Luhn algorithm."""
        if not npi or len(npi) != 10 or not npi.isdigit():
            return False
        
        # Luhn algorithm
        digits = [int(d) for d in npi]
        check_digit = digits[-1]
        digits = digits[:-1]
        
        # Double every other digit
        for i in range(len(digits) - 1, -1, -2):
            digits[i] *= 2
            if digits[i] > 9:
                digits[i] -= 9
        
        # Sum and check
        total = sum(digits)
        return (total * 9) % 10 == check_digit
    
    def _validate_dates(self, obj: Any) -> Any:
        """Validate and standardize date formats."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if 'date' in key.lower() or 'year' in key.lower():
                    if isinstance(value, str):
                        # Try to parse and standardize date
                        try:
                            if len(value) == 4 and value.isdigit():
                                # Just a year
                                obj[key] = value
                            else:
                                # Try to parse as date
                                dt = datetime.strptime(value[:10], '%Y-%m-%d')
                                obj[key] = dt.strftime('%Y-%m-%d')
                        except:
                            # Leave as is if can't parse
                            pass
                else:
                    obj[key] = self._validate_dates(value)
        elif isinstance(obj, list):
            return [self._validate_dates(item) for item in obj]
        
        return obj
    
    def _remove_empty(self, obj: Any) -> Any:
        """Remove empty values from the profile."""
        if isinstance(obj, dict):
            return {k: self._remove_empty(v) for k, v in obj.items() 
                   if v and (not isinstance(v, (list, dict)) or len(v) > 0)}
        elif isinstance(obj, list):
            return [self._remove_empty(item) for item in obj if item]
        return obj
    
    def _get_empty_profile(self) -> Dict:
        """Get an empty profile structure."""
        return {
            'basic_info': {
                'name': '',
                'npi': '',
                'specialty': '',
                'subspecialties': []
            },
            'professional': {
                'current_positions': [],
                'previous_positions': [],
                'hospital_affiliations': [],
                'practice_locations': []
            },
            'education': {
                'medical_school': {},
                'residency': [],
                'fellowship': [],
                'certifications': []
            },
            'industry_relationships': {
                'pharmaceutical': [],
                'medical_device': [],
                'healthcare_startups': [],
                'consulting': []
            },
            'research': {
                'publications': [],
                'clinical_trials': [],
                'research_interests': [],
                'grants': []
            },
            'leadership': {
                'board_memberships': [],
                'advisory_roles': [],
                'committee_positions': []
            },
            'professional_activities': {
                'speaking_engagements': [],
                'professional_societies': [],
                'awards_recognitions': []
            }
        }