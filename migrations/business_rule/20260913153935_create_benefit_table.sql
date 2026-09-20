-- Add migration script here
CREATE TABLE benefit (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `evaluator_id` BIGINT NOT NULL,
    `customer_id` BIGINT NOT NULL,
    `value` JSON NULL,
    `description` TEXT NULL,
    `expired_at` TIMESTAMP NULL,
    `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    `idempotency_key` VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (id),
    CONSTRAINT fk_benefit_evaluator
        FOREIGN KEY (evaluator_id)
        REFERENCES evaluator(id),
    UNIQUE KEY uq_benefit_idempotency (idempotency_key)
);