-- Add migration script here
CREATE TABLE evaluator (
    `id` BIGINT AUTO_INCREMENT,
    `name` TEXT NOT NULL,
    `start_valid_date` TIMESTAMP NULL,
    `end_valid_date` TIMESTAMP NULL,
    `is_active` BOOLEAN DEFAULT FALSE,
    `running_frequency` ENUM('MONTHLY', 'DAILY', 'YEARLY') NULL,
    `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    `created_by` BIGINT NULL,
    `updated_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `updated_by` BIGINT NULL,
    `deleted_at` TIMESTAMP NULL,
    `deleted_by` BIGINT NULL,

    PRIMARY KEY (id)
);