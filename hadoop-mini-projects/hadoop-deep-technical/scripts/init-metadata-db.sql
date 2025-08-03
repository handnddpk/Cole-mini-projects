-- Initialize HDFS metadata database for MinIO simulation
-- This schema simulates the core metadata structures that HDFS NameNode manages

-- Drop existing tables if they exist
DROP TABLE IF EXISTS block_locations CASCADE;
DROP TABLE IF EXISTS file_blocks CASCADE;
DROP TABLE IF EXISTS directory_entries CASCADE;
DROP TABLE IF EXISTS inode_metadata CASCADE;
DROP TABLE IF EXISTS namespace_tree CASCADE;

-- Create the main namespace tree table (simulates FSDirectory)
CREATE TABLE namespace_tree (
    path VARCHAR(4096) PRIMARY KEY,
    parent_path VARCHAR(4096),
    name VARCHAR(255) NOT NULL,
    is_directory BOOLEAN NOT NULL DEFAULT false,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    accessed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_parent_path (parent_path),
    INDEX idx_name (name)
);

-- Create inode metadata table (simulates INode attributes)
CREATE TABLE inode_metadata (
    path VARCHAR(4096) PRIMARY KEY,
    size_bytes BIGINT DEFAULT 0,
    block_size BIGINT DEFAULT 134217728, -- 128MB default
    replication_factor SMALLINT DEFAULT 3,
    owner VARCHAR(64) DEFAULT 'hdfs',
    group_name VARCHAR(64) DEFAULT 'hdfs',
    permissions VARCHAR(10) DEFAULT 'rwxr-xr-x',
    is_under_construction BOOLEAN DEFAULT false,
    generation_stamp BIGINT DEFAULT 0,
    FOREIGN KEY (path) REFERENCES namespace_tree(path) ON DELETE CASCADE
);

-- Create file blocks table (simulates BlockInfo)
CREATE TABLE file_blocks (
    block_id BIGINT PRIMARY KEY,
    file_path VARCHAR(4096) NOT NULL,
    block_index INTEGER NOT NULL,
    block_size BIGINT NOT NULL,
    generation_stamp BIGINT NOT NULL,
    minio_object_key VARCHAR(1024), -- Maps to actual MinIO object
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (file_path) REFERENCES namespace_tree(path) ON DELETE CASCADE,
    UNIQUE(file_path, block_index)
);

-- Create block locations table (simulates DatanodeDescriptor mapping)
CREATE TABLE block_locations (
    block_id BIGINT,
    minio_node VARCHAR(64), -- Which MinIO node stores this
    object_key VARCHAR(1024), -- Full MinIO object key
    is_corrupt BOOLEAN DEFAULT false,
    last_verified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (block_id, minio_node),
    FOREIGN KEY (block_id) REFERENCES file_blocks(block_id) ON DELETE CASCADE
);

-- Create directory entries cache (for fast directory listings)
CREATE TABLE directory_entries (
    parent_path VARCHAR(4096),
    child_name VARCHAR(255),
    child_path VARCHAR(4096),
    is_directory BOOLEAN,
    cache_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (parent_path, child_name),
    FOREIGN KEY (parent_path) REFERENCES namespace_tree(path) ON DELETE CASCADE
);

-- Create indexes for performance
CREATE INDEX idx_namespace_parent ON namespace_tree(parent_path);
CREATE INDEX idx_namespace_modified ON namespace_tree(modified_time);
CREATE INDEX idx_blocks_file ON file_blocks(file_path);
CREATE INDEX idx_locations_node ON block_locations(minio_node);
CREATE INDEX idx_directory_parent ON directory_entries(parent_path);

-- Insert root directory
INSERT INTO namespace_tree (path, parent_path, name, is_directory) 
VALUES ('/', NULL, '', true);

INSERT INTO inode_metadata (path, size_bytes, owner, group_name, permissions)
VALUES ('/', 0, 'hdfs', 'hdfs', 'rwxr-xr-x');

-- Create some sample directories to demonstrate the structure
INSERT INTO namespace_tree (path, parent_path, name, is_directory) VALUES
('/user', '/', 'user', true),
('/tmp', '/', 'tmp', true),
('/var', '/', 'var', true),
('/user/hdfs', '/user', 'hdfs', true);

INSERT INTO inode_metadata (path, size_bytes, owner, group_name, permissions) VALUES
('/user', 0, 'hdfs', 'hdfs', 'rwxr-xr-x'),
('/tmp', 0, 'hdfs', 'hdfs', 'rwxrwxrwt'),
('/var', 0, 'hdfs', 'hdfs', 'rwxr-xr-x'),
('/user/hdfs', 0, 'hdfs', 'hdfs', 'rwxr-xr-x');

-- Update directory entries cache
INSERT INTO directory_entries (parent_path, child_name, child_path, is_directory) VALUES
('/', 'user', '/user', true),
('/', 'tmp', '/tmp', true),
('/', 'var', '/var', true),
('/user', 'hdfs', '/user/hdfs', true);

-- Create a function to update modified time
CREATE OR REPLACE FUNCTION update_modified_time()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers to automatically update modified times
CREATE TRIGGER update_namespace_modified_time
    BEFORE UPDATE ON namespace_tree
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_time();

-- Create a function to maintain directory entries cache
CREATE OR REPLACE FUNCTION maintain_directory_cache()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Add to directory cache
        INSERT INTO directory_entries (parent_path, child_name, child_path, is_directory)
        VALUES (NEW.parent_path, NEW.name, NEW.path, NEW.is_directory)
        ON CONFLICT (parent_path, child_name) DO UPDATE SET
            child_path = NEW.path,
            is_directory = NEW.is_directory,
            cache_time = CURRENT_TIMESTAMP;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        -- Remove from directory cache
        DELETE FROM directory_entries 
        WHERE child_path = OLD.path;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for directory cache maintenance
CREATE TRIGGER maintain_directory_cache_trigger
    AFTER INSERT OR DELETE ON namespace_tree
    FOR EACH ROW
    EXECUTE FUNCTION maintain_directory_cache();

COMMIT;
