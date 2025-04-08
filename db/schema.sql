-- SQLite schema for Instalily Case Study

-- Contractors table
CREATE TABLE IF NOT EXISTS contractors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    rating REAL,
    address TEXT,
    phone TEXT,
    website TEXT,
    description TEXT,
    source TEXT,
    zip_code TEXT,
    city TEXT,
    state TEXT,
    processed_date TEXT,
    data_quality_score REAL,
    years_in_business INTEGER,
    estimated_size TEXT,
    high_value_prospect INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Certifications table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS certifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- Contractor certifications join table
CREATE TABLE IF NOT EXISTS contractor_certifications (
    contractor_id TEXT,
    certification_id INTEGER,
    PRIMARY KEY (contractor_id, certification_id),
    FOREIGN KEY (contractor_id) REFERENCES contractors(id) ON DELETE CASCADE,
    FOREIGN KEY (certification_id) REFERENCES certifications(id) ON DELETE CASCADE
);

-- Services table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- Contractor services join table
CREATE TABLE IF NOT EXISTS contractor_services (
    contractor_id TEXT,
    service_id INTEGER,
    PRIMARY KEY (contractor_id, service_id),
    FOREIGN KEY (contractor_id) REFERENCES contractors(id) ON DELETE CASCADE,
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);

-- Insights table
CREATE TABLE IF NOT EXISTS insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contractor_id TEXT NOT NULL,
    summary TEXT,
    engagement_strategy TEXT,
    contact_priority INTEGER,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contractor_id) REFERENCES contractors(id) ON DELETE CASCADE
);

-- Selling points table (one-to-many with insights)
CREATE TABLE IF NOT EXISTS selling_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    insight_id INTEGER NOT NULL,
    point TEXT NOT NULL,
    FOREIGN KEY (insight_id) REFERENCES insights(id) ON DELETE CASCADE
);

-- Recommended products table (one-to-many with insights)
CREATE TABLE IF NOT EXISTS recommended_products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    insight_id INTEGER NOT NULL,
    product TEXT NOT NULL,
    FOREIGN KEY (insight_id) REFERENCES insights(id) ON DELETE CASCADE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_contractors_high_value ON contractors(high_value_prospect);
CREATE INDEX IF NOT EXISTS idx_contractors_rating ON contractors(rating);
CREATE INDEX IF NOT EXISTS idx_contractors_zip ON contractors(zip_code);
CREATE INDEX IF NOT EXISTS idx_insights_priority ON insights(contact_priority);