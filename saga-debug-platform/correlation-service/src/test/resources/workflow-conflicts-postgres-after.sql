INSERT INTO saga_instances (saga_id, workflow_type) VALUES ('old-writer', 'ORDER_SAGA');
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM saga_instances WHERE workflow_conflicts <> '[]'::jsonb) THEN
        RAISE EXCEPTION 'Migration invented historical conflicts';
    END IF;
    BEGIN
        UPDATE saga_instances SET workflow_conflicts = '["ORDER_SAGA"]';
        RAISE EXCEPTION 'Singleton conflict was accepted';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    BEGIN
        UPDATE saga_instances SET workflow_conflicts = 'null';
        RAISE EXCEPTION 'Non-array conflict was accepted';
    EXCEPTION WHEN check_violation OR invalid_parameter_value THEN NULL;
    END;
    UPDATE saga_instances SET workflow_conflicts = '["LOAN_APPROVAL", "ORDER_SAGA"]';
    IF EXISTS (SELECT 1 FROM saga_instances WHERE jsonb_array_length(workflow_conflicts) <> 2) THEN
        RAISE EXCEPTION 'Valid conflict did not persist';
    END IF;
END $$;
ROLLBACK;
