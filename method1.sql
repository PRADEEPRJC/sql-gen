CREATE OR REPLACE PACKAGE BODY pkg_set_recovery_fields
IS
   PROCEDURE main
   IS
      v_batch_size CONSTANT PLS_INTEGER := 5000;
      v_error_claim VARCHAR2(50);
      
      TYPE t_claimant_key IS TABLE OF claimant.claimant_key%TYPE;
      TYPE t_recovery_date IS TABLE OF NUMBER(8);
      TYPE t_recovery_status IS TABLE OF VARCHAR2(1);
      
      v_claimant_keys t_claimant_key;
      v_recovery_dates t_recovery_date;
      v_recovery_statuses t_recovery_status;
   BEGIN
      -- Step 1: Update claimants that need to be closed
      -- (all recovery types are in closed status)
      MERGE INTO claimant tgt
      USING (
         SELECT 
            c.claimant_key,
            csc.transaction_date AS recovery_closed_date,
            'C' AS recovery_status
         FROM wt_check_claim_iot wt
         INNER JOIN business_unit_xref bux 
            ON wt.originating_business_unit = bux.business_unit
            AND bux.recovery_date_closed_rqd = 'Y'
         INNER JOIN claimant c
            ON c.originating_business_unit = wt.originating_business_unit
            AND c.claim_number = wt.claim_number
            AND c.claimant_number = wt.claimant_number
         INNER JOIN (
            -- Get the most recent transaction date for closed recovery types
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
            ON csc.business_unit = wt.originating_business_unit
            AND csc.claim_number = wt.claim_number
            AND csc.claimant_number = wt.claimant_number
            AND csc.rn = 1
         WHERE c.salvage_status IN ('C', 'F', 'W', 'N')
           AND c.subrogation_status IN ('C', 'F', 'W', 'N')
           AND c.tp_deductible_status IN ('C', 'F', 'W', 'N')
           AND NVL(c.recovery_date_closed, 0) = 0
      ) src
      ON (tgt.claimant_key = src.claimant_key)
      WHEN MATCHED THEN
         UPDATE SET 
            tgt.recovery_date_closed = src.recovery_closed_date,
            tgt.recovery_status = src.recovery_status;
      
      DBMS_OUTPUT.put_line('Closed records updated: ' || SQL%ROWCOUNT);
      
      -- Step 2: Update claimants that need to be reopened
      -- (at least one recovery type is NOT in closed status)
      UPDATE claimant c
      SET 
         recovery_date_closed = 0,
         recovery_status = CASE 
            WHEN c.recovery_status = 'C' THEN 'R' 
            ELSE 'O' 
         END
      WHERE c.claimant_key IN (
         SELECT c2.claimant_key
         FROM wt_check_claim_iot wt
         INNER JOIN business_unit_xref bux 
            ON wt.originating_business_unit = bux.business_unit
            AND bux.recovery_date_closed_rqd = 'Y'
         INNER JOIN claimant c2
            ON c2.originating_business_unit = wt.originating_business_unit
            AND c2.claim_number = wt.claim_number
            AND c2.claimant_number = wt.claimant_number
         WHERE NOT (
            c2.salvage_status IN ('C', 'F', 'W', 'N')
            AND c2.subrogation_status IN ('C', 'F', 'W', 'N')
            AND c2.tp_deductible_status IN ('C', 'F', 'W', 'N')
         )
         AND (c2.recovery_date_closed <> 0 OR c2.recovery_status = 'C')
      );
      
      DBMS_OUTPUT.put_line('Reopened records updated: ' || SQL%ROWCOUNT);
      
      -- Step 3: Insert history records for all changed claimants
      INSERT INTO claimant_hist_hold (
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
         NVL(csc.max_transaction_date, TO_NUMBER(TO_CHAR(TRUNC(SYSDATE), 'yyyymmdd'))),
         pcw_dim_claimant_seq.NEXTVAL,
         c.expense_status, c.expense_date_opened, c.expense_date_closed,
         c.recovery_status, c.recovery_date_closed, c.bep_opened_first,
         c.overall_status, c.overall_date_closed, c.overall_bep_closed, c.genius_risk_code
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
      ) csc
         ON csc.business_unit = wt.originating_business_unit
         AND csc.claim_number = wt.claim_number
         AND csc.claimant_number = wt.claimant_number
      WHERE (
         -- Records that were closed
         (c.salvage_status IN ('C', 'F', 'W', 'N')
          AND c.subrogation_status IN ('C', 'F', 'W', 'N')
          AND c.tp_deductible_status IN ('C', 'F', 'W', 'N')
          AND EXISTS (
             SELECT 1 FROM claim_stage_consol csc2
             WHERE csc2.business_unit = wt.originating_business_unit
               AND csc2.claim_number = wt.claim_number
               AND csc2.claimant_number = wt.claimant_number
               AND csc2.transaction_code IN ('7', '8', '11')
          ))
         OR
         -- Records that were reopened
         (NOT (c.salvage_status IN ('C', 'F', 'W', 'N')
               AND c.subrogation_status IN ('C', 'F', 'W', 'N')
               AND c.tp_deductible_status IN ('C', 'F', 'W', 'N'))
          AND (c.recovery_date_closed <> 0 OR c.recovery_status = 'C'))
      );
      
      DBMS_OUTPUT.put_line('History records inserted: ' || SQL%ROWCOUNT);
      
      COMMIT;
      
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
         DBMS_OUTPUT.put_line('WHEN OTHERS Error in main: ' || SUBSTR(SQLERRM, 1, 200));
         IF v_error_claim IS NOT NULL THEN
            DBMS_OUTPUT.put_line('Problem with claim_number: ' || v_error_claim);
         END IF;
         RAISE_APPLICATION_ERROR(-20039, 
            'WHEN OTHERS: Fatal Errors have occurred during processing. EXECUTION STOPPED');
   END main;
END pkg_set_recovery_fields;


--
CREATE INDEX idx_wt_check_bu ON wt_check_claim_iot(originating_business_unit);
CREATE INDEX idx_claimant_lookup ON claimant(originating_business_unit, claim_number, claimant_number);
CREATE INDEX idx_csc_lookup ON claim_stage_consol(business_unit, claim_number, claimant_number, transaction_code);
CREATE INDEX idx_bux_recovery ON business_unit_xref(business_unit, recovery_date_closed_rqd);