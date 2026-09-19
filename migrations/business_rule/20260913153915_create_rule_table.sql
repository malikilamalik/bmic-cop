-- Add migration script here
CREATE TABLE rule (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `evaluator_id` BIGINT NOT NULL,
    `content` JSON NOT NULL,
    `input` JSON NOT NULL,
    `version` INT NULL,
    `description` VARCHAR(255) NULL,
    `is_active` BOOLEAN DEFAULT TRUE,
    `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    `created_by` BIGINT NULL,

    PRIMARY KEY (id),
    CONSTRAINT fk_rule_evaluator
        FOREIGN KEY (evaluator_id)
        REFERENCES evaluator(id),
    CONSTRAINT uq_rule_evaluator_version
        UNIQUE (evaluator_id, version)
);