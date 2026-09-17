CREATE TABLE job_detail (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id BIGINT UNSIGNED NOT NULL,
    entity VARCHAR(100) NOT NULL,
    `key` VARCHAR(255) NOT NULL,
    file_start_range DATETIME NULL,
    file_end_range DATETIME NULL,

    PRIMARY KEY (id),

    INDEX idx_job_detail_job_id (job_id),

    CONSTRAINT fk_job_detail_job
        FOREIGN KEY (job_id)
        REFERENCES job(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);