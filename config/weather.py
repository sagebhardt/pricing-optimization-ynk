"""Weather configuration: region coordinates for Open-Meteo API."""

# Chilean regions where stores operate → (latitude, longitude)
# Coordinates are approximate city centers
REGION_COORDS = {
    "RM: Metropolitana de": (-33.45, -70.65),    # Santiago
    "V: de Valparaíso": (-33.05, -71.62),        # Valparaíso / Viña del Mar
    "VIII: del BioBío": (-36.82, -73.05),         # Concepción
    "VII: del Maule": (-35.43, -71.66),           # Talca
    "IX: de la Araucanía": (-38.74, -72.60),      # Temuco
    "X: de Los Lagos": (-41.47, -72.94),          # Puerto Montt
    "XIV: de los Ríos": (-39.81, -73.24),         # Valdivia
    "VI: del Libertador G": (-34.17, -70.74),     # Rancagua
    "IV: de Coquimbo": (-29.95, -71.34),          # La Serena
    "II: de Antofagasta": (-23.65, -70.40),       # Antofagasta
    "I: de Tarapacá": (-20.21, -70.13),           # Iquique
    "XV: de Arica y Parin": (-18.47, -70.31),     # Arica
    "III: de Atacama": (-27.37, -70.33),           # Copiapó
}

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum"
