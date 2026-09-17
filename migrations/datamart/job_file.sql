CREATE TABLE job_file (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_detail_id BIGINT UNSIGNED NOT NULL,
    filename VARCHAR(255) NOT NULL,
    status ENUM(
        'PROCESSING',
        'ERROR',
        'COMPLETED'
    ) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),

    INDEX idx_job_file_job_detail_id (job_detail_id),

    CONSTRAINT fk_job_file_job_detail
        FOREIGN KEY (job_detail_id)
        REFERENCES job_detail(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);