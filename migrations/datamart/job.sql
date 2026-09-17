CREATE TABLE job (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    evaluator_id BIGINT UNSIGNED NOT NULL,
    status ENUM(
        'PROCESSING',
        'EVALUATING',
        'ERROR',
        'COMPLETED'
    ) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

    INDEX idx_job_evaluator_id (evaluator_id),
    INDEX idx_job_status (status)
);