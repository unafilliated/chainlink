-- +goose Up
-- +goose StatementBegin
ALTER TABLE keeper_specs ADD COLUMN IF NOT EXISTS contract_version text NOT NULL DEFAULT '1.0.0';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE keeper_specs DROP COLUMN contract_version;
-- +goose StatementEnd
