-- Step 1: Create Global Temporary Tables (run once)
CREATE GLOBAL TEMPORARY TABLE gtt_recovery_updates (
   claimant_key NUMBER,
   claim_number VARCHAR2(50),
   claimant_number NUMBER,
   originating_business_unit VARCHAR2(10),
   recovery_date_closed NUMBER(8),
   recovery_status VARCHAR2(1),
   valid_from_date NUMBER(8),
   update_type VARCHAR2(1) -- 'C' for Close, 'R' for Reopen
) ON COMMIT PRESERVE ROWS;

CREATE INDEX gtt_idx_claimant_key ON gtt_recovery_updates(claimant_key);
CREATE INDEX gtt_idx_update_type ON gtt_recovery_updates(update_type);

-- Step 2: Updated Package Body
CREATE OR REPLACE PACKAGE BODY pkg_set_recovery_fields
IS
   PROCEDURE main
   IS
      v_error_claim VARCHAR2(50);
      v_closed_count NUMBER := 0;
      v_reopened_count NUMBER := 0;
      v_history_count NUMBER := 0;
   BEGIN
      -- Clear GTT in case of previous failed run
      DELETE FROM gtt_recovery_updates;
      
      -- Step 1: Identify and stage claimants that need to be CLOSED
      INSERT INTO gtt_recovery_updates (
         claimant_key, claim_number, claimant_number, originating_business_unit,
         recovery_date_closed, recovery_status, valid_from_date, update_type
      )
      SELECT 
         c.claimant_key,
         c.claim_number,
         c.claimant_number,
         c.originating_business_unit,
         csc.transaction_date AS recovery_date_closed,
         'C' AS recovery_status,
         NVL(csc_max.max_transaction_date, TO_NUMBER(TO_CHAR(TRUNC(SYSDATE), 'yyyymmdd'))) AS valid_from_date,
         'C' AS update_type
      FROM wt_check_claim_iot wt
      INNER JOIN business_unit_xref bux 
         ON wt.originating_business_unit = bux.business_unit
         AND bux.recovery_date_closed_rqd = 'Y'
      INNER JOIN claimant c
         ON c.originating_business_unit = wt.originating_business_unit
         AND c.claim_number = wt.claim_number
         AND c.claimant_number = wt.claimant_number
      INNER JOIN (
         SELECT 
            business_unit,
            claim_number,
            claimant_number,
            transaction_date,
            ROW_NUMBER() OVER (
               PARTITION BY business_unit, claim_number, claimant_number 
               ORDER BY accounting_dt DESC, voucher_number DESC
            ) AS rn
         FROM claim_stage_consol
         WHERE transaction_code IN ('7', '8', '11')
      ) csc
         ON csc.business_unit = c.originating_business_unit
         AND csc.claim_number = c.claim_number
         AND csc.claimant_number = c.claimant_number
         AND csc.rn = 1
      LEFT JOIN (
         SELECT 
            business_unit,
            claim_number,
            claimant_number,
            MAX(transaction_date) AS max_transaction_date
         FROM claim_stage_consol
         GROUP BY business_unit, claim_number, claimant_number
      ) csc_max
         ON csc_max.business_unit = c.originating_business_unit
         AND csc_max.claim_number = c.claim_number
         AND csc_max.claimant_number = c.claimant_number
      WHERE c.salvage_status IN ('C', 'F', 'W', 'N')
        AND c.subrogation_status IN ('C', 'F', 'W', 'N')
        AND c.tp_deductible_status IN ('C', 'F', 'W', 'N')
        AND NVL(c.recovery_date_closed, 0) = 0;
      
      v_closed_count := SQL%ROWCOUNT;
      DBMS_OUTPUT.put_line('Staged for closing: ' || v_closed_count);
      
      -- Step 2: Identify and stage claimants that need to be REOPENED
      INSERT INTO gtt_recovery_updates (
         claimant_key, claim_number, claimant_number, originating_business_unit,
         recovery_date_closed, recovery_status, valid_from_date, update_type
      )
      SELECT 
         c.claimant_key,
         c.claim_number,
         c.claimant_number,
         c.originating_business_unit,
         0 AS recovery_date_closed,
         CASE WHEN c.recovery_status = 'C' THEN 'R' ELSE 'O' END AS recovery_status,
         NVL(csc_max.max_transaction_date, TO_NUMBER(TO_CHAR(TRUNC(SYSDATE), 'yyyymmdd'))) AS valid_from_date,
         'R' AS update_type
      FROM wt_check_claim_iot wt
      INNER JOIN business_unit_xref bux 
         ON wt.originating_business_unit = bux.business_unit
         AND bux.recovery_date_closed_rqd = 'Y'
      INNER JOIN claimant c
         ON c.originating_business_unit = wt.originating_business_unit
         AND c.claim_number = wt.claim_number
         AND c.claimant_number = wt.claimant_number
      LEFT JOIN (
         SELECT 
            business_unit,
            claim_number,
            claimant_number,
            MAX(transaction_date) AS max_transaction_date
         FROM claim_stage_consol
         GROUP BY business_unit, claim_number, claimant_number
      ) csc_max
         ON csc_max.business_unit = c.originating_business_unit
         AND csc_max.claim_number = c.claim_number
         AND csc_max.claimant_number = c.claimant_number
      WHERE NOT (
         c.salvage_status IN ('C', 'F', 'W', 'N')
         AND c.subrogation_status IN ('C', 'F', 'W', 'N')
         AND c.tp_deductible_status IN ('C', 'F', 'W', 'N')
      )
      AND (c.recovery_date_closed <> 0 OR c.recovery_status = 'C');
      
      v_reopened_count := SQL%ROWCOUNT;
      DBMS_OUTPUT.put_line('Staged for reopening: ' || v_reopened_count);
      
      -- Step 3: Perform bulk update on claimant table
      MERGE INTO claimant tgt
      USING gtt_recovery_updates src
      ON (tgt.claimant_key = src.claimant_key)
      WHEN MATCHED THEN
         UPDATE SET 
            tgt.recovery_date_closed = src.recovery_date_closed,
            tgt.recovery_status = src.recovery_status;
      
      DBMS_OUTPUT.put_line('Claimant records updated: ' || SQL%ROWCOUNT);
      
      -- Step 4: Bulk insert into history table
      INSERT /*+ APPEND */ INTO claimant_hist_hold (
         claimant_key, claim_number, claimant_number, originating_business_unit,
         claimant_id, claimant_name, claimant_status, condition_injury_damage,
         current_coverage_code, current_geo_code, current_product, driver_pilot,
         driver_pilot_age, last_activity_date, license_vin_number, litigation_indicator,
         make_model_vehicle, plant_division, policy_holder_reference, policy_segment,
         product_type, salvage_status, subrogation_status, tp_deductible_status,
         type_of_claim, usi_claim_no_reformat, usi_claimant_no_reformat, 
         usi_policy_no_reformat, date_opened, date_closed, bep_opened, bep_closed,
         policy_coverage_key, certificate_number, date_of_insurance, commodity,
         commodity_code, conveyance, conveyance_code, departure_point,
         departure_point_code, destination, destination_code, departure_date,
         arrival_date, vessel_name, flag, carrier_line, current_local_line_of_business,
         insrd_ticketed_by_police, insrd_drvng_under_the_inflnce, road_condition,
         claimant_client_data01, claimant_client_data02, claimant_client_data03,
         claimant_client_data04, claimant_client_date01, claimant_client_date02,
         mcsi_upd_date, compensation_rate, days_worked, dependant, waiting_period,
         certificate_number_1, certificate_number_2, claimant_reference,
         claimant_reserve_curr, responsible_adjuster, claimant_type, age_at_claim,
         valid_from_date, sequence_number, expense_status, expense_date_opened,
         expense_date_closed, recovery_status, recovery_date_closed, bep_opened_first,
         overall_status, overall_date_closed, overall_bep_closed, genius_risk_code
      )
      SELECT 
         c.claimant_key, c.claim_number, c.claimant_number, c.originating_business_unit,
         c.claimant_id, c.claimant_name, c.claimant_status, c.condition_injury_damage,
         c.current_coverage_code, c.current_geo_code, c.current_product, c.driver_pilot,
         c.driver_pilot_age, c.last_activity_date, c.license_vin_number, c.litigation_indicator,
         c.make_model_vehicle, c.plant_division, c.policy_holder_reference, c.policy_segment,
         c.product_type, c.salvage_status, c.subrogation_status, c.tp_deductible_status,
         c.type_of_claim, c.usi_claim_no_reformat, c.usi_claimant_no_reformat,
         c.usi_policy_no_reformat, c.date_opened, c.date_closed, c.bep_opened, c.bep_closed,
         c.policy_coverage_key, c.certificate_number, c.date_of_insurance, c.commodity,
         c.commodity_code, c.conveyance, c.conveyance_code, c.departure_point,
         c.departure_point_code, c.destination, c.destination_code, c.departure_date,
         c.arrival_date, c.vessel_name, c.flag, c.carrier_line, c.current_local_line_of_business,
         c.insrd_ticketed_by_police, c.insrd_drvng_under_the_inflnce, c.road_condition,
         c.claimant_client_data01, c.claimant_client_data02, c.claimant_client_data03,
         c.claimant_client_data04, c.claimant_client_date01, c.claimant_client_date02,
         c.mcsi_upd_date, c.compensation_rate, c.days_worked, c.dependant, c.waiting_period,
         c.certificate_number_1, c.certificate_number_2, c.claimant_reference,
         c.claimant_reserve_curr, c.responsible_adjuster, c.claimant_type, c.age_at_claim,
         gtt.valid_from_date,
         pcw_dim_claimant_seq.NEXTVAL,
         c.expense_status, c.expense_date_opened, c.expense_date_closed,
         gtt.recovery_status, gtt.recovery_date_closed, c.bep_opened_first,
         c.overall_status, c.overall_date_closed, c.overall_bep_closed, c.genius_risk_code
      FROM gtt_recovery_updates gtt
      INNER JOIN claimant c
         ON c.claimant_key = gtt.claimant_key;
      
      v_history_count := SQL%ROWCOUNT;
      DBMS_OUTPUT.put_line('History records inserted: ' || v_history_count);
      
      -- Clear GTT before commit
      DELETE FROM gtt_recovery_updates;
      
      COMMIT;
      
      DBMS_OUTPUT.put_line('Process completed successfully');
      DBMS_OUTPUT.put_line('Summary - Closed: ' || v_closed_count || 
                          ', Reopened: ' || v_reopened_count || 
                          ', History: ' || v_history_count);
      
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
         DELETE FROM gtt_recovery_updates; -- Clean up GTT
         DBMS_OUTPUT.put_line('WHEN OTHERS Error in main: ' || SUBSTR(SQLERRM, 1, 200));
         IF v_error_claim IS NOT NULL THEN
            DBMS_OUTPUT.put_line('Problem with claim_number: ' || v_error_claim);
         END IF;
         RAISE_APPLICATION_ERROR(-20039, 
            'WHEN OTHERS: Fatal Errors have occurred during processing. EXECUTION STOPPED');
   END main;
END pkg_set_recovery_fields;
