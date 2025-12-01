CREATE OR REPLACE PACKAGE BODY pkg_set_recovery_fields
IS
   v_history_reqd           VARCHAR2 (1);
   v_recovery_status        VARCHAR2 (1);
   v_recovery_closed_date   NUMBER (8);
   v_valid_from             claim_hist_hold.valid_from_date%TYPE;
   v_claim_num              claim.claim_number%TYPE;

   PROCEDURE main
   IS
      v_trans_date     NUMBER (8);
      v_count          INTEGER := 0;

   BEGIN
      -- Step 1: Update recovery fields for claimants meeting the criteria
      -- Using MERGE for better performance and readability
      
      MERGE INTO claimant c
      USING (
         SELECT DISTINCT
                wt.originating_business_unit,
                wt.claim_number,
                wt.claimant_number,
                clm.claimant_key
           FROM wt_check_claim_iot wt
           INNER JOIN claimant clm 
              ON clm.originating_business_unit = wt.originating_business_unit
              AND clm.claim_number = wt.claim_number
              AND clm.claimant_number = wt.claimant_number
           INNER JOIN business_unit_xref bx 
              ON bx.business_unit = wt.originating_business_unit
              AND bx.recovery_date_closed_rqd = 'Y'
          WHERE clm.salvage_status IN ('C', 'F', 'W', 'N')
            AND clm.subrogation_status IN ('C', 'F', 'W', 'N')
            AND clm.tp_deductible_status IN ('C', 'F', 'W', 'N')
            AND NVL(clm.recovery_date_closed, 0) = 0
      ) src
      ON (c.claimant_key = src.claimant_key)
      WHEN MATCHED THEN
         UPDATE SET
            recovery_date_closed = (
               SELECT transaction_date
                 FROM (
                    SELECT transaction_date
                      FROM claim_stage_consol
                     WHERE business_unit = src.originating_business_unit
                       AND claim_number = src.claim_number
                       AND claimant_number = src.claimant_number
                       AND transaction_code IN ('7', '8', '11')
                     ORDER BY accounting_dt DESC, voucher_number DESC
                     FETCH FIRST 1 ROW ONLY
                 )
            ),
            recovery_status = 'C';

      -- Step 2: Update recovery fields for claimants NOT meeting the criteria
      MERGE INTO claimant c
      USING (
         SELECT DISTINCT
                clm.claimant_key,
                clm.recovery_date_closed,
                clm.recovery_status
           FROM wt_check_claim_iot wt
           INNER JOIN claimant clm 
              ON clm.originating_business_unit = wt.originating_business_unit
              AND clm.claim_number = wt.claim_number
              AND clm.claimant_number = wt.claimant_number
           INNER JOIN business_unit_xref bx 
              ON bx.business_unit = wt.originating_business_unit
              AND bx.recovery_date_closed_rqd = 'Y'
          WHERE (clm.salvage_status NOT IN ('C', 'F', 'W', 'N')
             OR clm.subrogation_status NOT IN ('C', 'F', 'W', 'N')
             OR clm.tp_deductible_status NOT IN ('C', 'F', 'W', 'N'))
            AND (clm.recovery_date_closed <> 0 OR clm.recovery_status = 'C')
      ) src
      ON (c.claimant_key = src.claimant_key)
      WHEN MATCHED THEN
         UPDATE SET
            recovery_date_closed = 0,
            recovery_status = CASE 
                               WHEN src.recovery_status = 'C' THEN 'R'
                               ELSE 'O'
                              END;

      -- Step 3: Insert history records for all changed records
      INSERT INTO claimant_hist_hold
      (
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
         arrival_date, vessel_name, flag, carrier_line,
         current_local_line_of_business, insrd_ticketed_by_police,
         insrd_drvng_under_the_inflnce, road_condition, claimant_client_data01,
         claimant_client_data02, claimant_client_data03, claimant_client_data04,
         claimant_client_date01, claimant_client_date02, mcsi_upd_date,
         compensation_rate, days_worked, dependant, waiting_period,
         certificate_number_1, certificate_number_2, claimant_reference,
         claimant_reserve_curr, responsible_adjuster, claimant_type,
         age_at_claim, valid_from_date, sequence_number, expense_status,
         expense_date_opened, expense_date_closed, recovery_status,
         recovery_date_closed, bep_opened_first, overall_status,
         overall_date_closed, overall_bep_closed, genius_risk_code
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
         c.arrival_date, c.vessel_name, c.flag, c.carrier_line,
         c.current_local_line_of_business, c.insrd_ticketed_by_police,
         c.insrd_drvng_under_the_inflnce, c.road_condition, c.claimant_client_data01,
         c.claimant_client_data02, c.claimant_client_data03, c.claimant_client_data04,
         c.claimant_client_date01, c.claimant_client_date02, c.mcsi_upd_date,
         c.compensation_rate, c.days_worked, c.dependant, c.waiting_period,
         c.certificate_number_1, c.certificate_number_2, c.claimant_reference,
         c.claimant_reserve_curr, c.responsible_adjuster, c.claimant_type,
         c.age_at_claim,
         NVL((SELECT MAX(h.transaction_date)
                FROM claim_stage_consol h
               WHERE h.business_unit = c.originating_business_unit
                 AND h.claim_number = c.claim_number
                 AND h.claimant_number = c.claimant_number),
             TO_NUMBER(TO_CHAR(TRUNC(SYSDATE), 'yyyymmdd'))),
         pcw_dim_claimant_seq.NEXTVAL,
         c.expense_status, c.expense_date_opened, c.expense_date_closed,
         c.recovery_status, c.recovery_date_closed,
         c.bep_opened_first, c.overall_status, c.overall_date_closed,
         c.overall_bep_closed, c.genius_risk_code
      FROM claimant c
      INNER JOIN wt_check_claim_iot wt 
         ON wt.originating_business_unit = c.originating_business_unit
         AND wt.claim_number = c.claim_number
         AND wt.claimant_number = c.claimant_number
      INNER JOIN business_unit_xref bx 
         ON bx.business_unit = wt.originating_business_unit
         AND bx.recovery_date_closed_rqd = 'Y'
      WHERE (
         -- Condition 1: All recovery statuses are in valid state and recovery_date_closed was NULL
         (c.salvage_status IN ('C', 'F', 'W', 'N')
          AND c.subrogation_status IN ('C', 'F', 'W', 'N')
          AND c.tp_deductible_status IN ('C', 'F', 'W', 'N')
          AND NVL(c.recovery_date_closed, 0) = 0)
         OR
         -- Condition 2: Any recovery status is invalid and recovery_date_closed or recovery_status changed
         (
          (c.salvage_status NOT IN ('C', 'F', 'W', 'N')
           OR c.subrogation_status NOT IN ('C', 'F', 'W', 'N')
           OR c.tp_deductible_status NOT IN ('C', 'F', 'W', 'N'))
          AND (c.recovery_date_closed <> 0 OR c.recovery_status = 'C')
         )
      );

      v_count := SQL%ROWCOUNT;

      COMMIT;

      DBMS_OUTPUT.put_line('Successfully processed ' || v_count || ' history records');

   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         DBMS_OUTPUT.put_line('Error in main: ' || SUBSTR(SQLERRM, 1, 200));
         DBMS_OUTPUT.put_line('Problem with claim_number: ' || v_claim_num);
         RAISE_APPLICATION_ERROR(
            -20039,
            'WHEN OTHERS: Fatal Errors have occurred during processing. EXECUTION STOPPED'
         );
   END main;

END pkg_set_recovery_fields;
