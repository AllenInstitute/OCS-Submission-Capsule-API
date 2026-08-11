-- Optimized ~April 2026. If there are questions contact BI Core and/or Nicholas Mei (Data & Solutions 'Marten' Team)
-- General strategy
-- 1. Always Be Constraining (ABC): Try to constrain results of Common Table Expressions (CTE) as *early* and as significantly as possible.
--    The fewer tables and rows that a CTE has to consider, the less time and memory the overall query will take
-- 2. Make heavy use of `EXPLAIN (ANALYZE, BUFFERS)`: It can help narrow down CTEs and JOINs that are not efficient 
--    -  Rough memory usage can be estimated by `Memory (bytes) = actual rows * width
--EXPLAIN (ANALYZE, BUFFERS)
WITH
	core_cte AS (
		SELECT
			-- rseq_experiment_components
			rec.name AS exp_component_name,
			rec.name_from_vendor AS exp_component_vendor_name,
			rec.vendor_read_count AS vendor_read_count,
			rec.failed AS experiment_component_failed,
			rec.sample_id,
			rec.rseq_library_prep_id,
			rec.organism_id_for_alignment,
			rec.id AS rseq_experiment_component_id,
			rec.cell_capture AS expc_cell_capture,
			-- rseq_experiments
			re.cluster_density_thousands_per_mm2 AS exp_cluster_density_thousands_per_mm2,
			re.vendor_read_count AS lane_read_count,
			-- rseq_tubes
			rt.id AS tube_id,
			rt.name AS tube,
			rt.internal_name AS tube_internal_name,
			rt.content_concentration_nm AS tube_contents_nm,
			rt.concentration_from_vendor_nm AS tube_contents_nm_from_vendor,
			rt.avg_size_bp AS tube_avg_size_bp,
			-- rseq_tube_sets
			rts.name AS batch,
			rts.name_from_vendor AS batch_vendor_name,
			rts.storage_directory,
			rts.sent_to_vendor_at AS tube_set_sent_to_vendor_date,
			-- rna_amplification_inputs
			rai.sample_quantity_count,
			rai.sample_quantity_pg,
			-- rna_amplifications
			ra.name AS rna_amplification,
			ra.cycles AS pcr_cycles,
			ra.percent_cdna_longer_than_400bp,
			ra.run_date as amp_date,
			CASE ra.failed WHEN 'f' THEN 'Pass' WHEN 't' THEN 'FAIL' END AS rna_amplification_pass_fail,
			ra.amplified_quantity_ng,
			ra.load_name AS load_name,
			ra.port_well AS port_well,
			-- rna_amplification_methods
			ram.name AS method,
			-- rna_amplification_sets
			ras.name AS rna_amplification_set,
			-- rseq_library_preps
			rlp.run_date AS lib_date,
			rlp.input_quantity_ng AS library_input_ng,
			rlp.avg_size_bp,
			rlp.quantification2_ng,
			rlp.quantification_fmol,
			rlp.quantification2_nm,
			rlp.name AS library_prep,
			CASE rlp.fail WHEN 'f' THEN 'Pass' WHEN 't' THEN 'FAIL' END AS library_prep_pass_fail,
			-- rseq_library_prep_sets
			rlps.name AS library_prep_set,
			-- rseq_library_prep_methods
			rlpm.name AS lib_method,
			-- facs_well_templates
			fwt.id AS facs_well_templates_id,
			fwt.name AS sample_name,
			-- facs_plate_templates
			fpt.name AS facs_container,
			-- facs_population_plans
			fpp.name AS facs_population_plan,
			-- rseq_tube_inputs
			rti.input_quantity_fmol AS tube_input_fmol,
			-- sequences
			r1_idx.name AS r1_index,
			r2_idx.name AS r2_index,
			-- rseq_oligos
			r1_seq.sequence_data || '-' || r2_seq.sequence_data AS index_sequence_pair,
			-- external_controls
			ec.name AS control
		FROM rseq_experiment_components rec
		JOIN rseq_experiments re ON re.id = rec.rseq_experiment_id
		JOIN rseq_tubes rt ON rt.id = re.rseq_tube_id
		JOIN rseq_tube_sets rts ON rts.id = rt.rseq_tube_set_id
		JOIN rna_amplification_inputs rai ON rai.sample_id = rec.sample_id 
		JOIN rna_amplifications ra ON ra.id = rai.rna_amplification_id
		JOIN rna_amplification_methods ram ON ram.id = ra.rna_amplification_method_id
		JOIN rna_amplification_sets ras ON ras.id = ra.rna_amplification_set_id
		JOIN rseq_library_preps rlp ON rlp.input_id = ra.id AND rlp.id = rec.rseq_library_prep_id
		JOIN rseq_library_prep_sets rlps ON rlps.id = rlp.rseq_library_prep_set_id
		JOIN rseq_library_prep_methods rlpm ON rlpm.id = rlp.rseq_library_prep_method_id
		LEFT JOIN facs_well_templates fwt ON fwt.id = rec.sample_id
		LEFT JOIN facs_plate_templates fpt ON fpt.id = fwt.facs_plate_id
		LEFT JOIN facs_population_plans fpp ON fpp.id = fwt.facs_population_plan_id
		LEFT JOIN rseq_tube_inputs rti ON rti.input_id = rlp.id AND rti.rseq_tube_id = rt.id
		LEFT JOIN rseq_oligos r1_idx ON r1_idx.id = rlp.read1_index_id
		LEFT JOIN "sequences" r1_seq ON r1_seq.id = r1_idx.sequence_id
		LEFT JOIN rseq_oligos r2_idx ON r2_idx.id = rlp.read2_index_id
		LEFT JOIN "sequences" r2_seq ON r2_seq.id = r2_idx.sequence_id
		LEFT JOIN external_controls ec ON ec.id = fwt.external_control_id
		-- Template strings are filled by psycopg. Query keys are `batch_names`, `fastq_names`, and `load_names`
		WHERE rts.name_from_vendor = ANY(ARRAY[%(batch_names)s])
			OR rec.name = ANY(ARRAY[%(fastq_names)s])
			OR ra.load_name = ANY(ARRAY[%(load_names)s])
		-- Some example queries with concrete values
		-- WHERE rec.name = ANY(ARRAY['NW-TX4108-2']) OR ra.load_name = ANY(ARRAY[NULL])
		-- WHERE rec.name = ANY(ARRAY[NULL]) OR ra.load_name = ANY(ARRAY['2992_A04'])
		-- WHERE rec.name = ANY(ARRAY[NULL]) OR ra.load_name = ANY(ARRAY['2952_A02'])
		-- WHERE rec.name = ANY(ARRAY[NULL]) OR ra.load_name = ANY(ARRAY['2186_A08'])
		-- WHERE rec.name = ANY(ARRAY['NY-MX22048-6']) OR ra.load_name = ANY(ARRAY[NULL])
		-- WHERE rec.name = ANY(ARRAY['SM-IYWLH_S347_E1-50']) OR ra.load_name = ANY(ARRAY[NULL])
	),
    sample_ids_cte AS (
        SELECT DISTINCT sample_id
        FROM core_cte
    ),
	analysis_run_metadata_cte AS (
		SELECT
			cc.sample_id,
			cc.rseq_library_prep_id,
			cc.tube_id,
	        o.common_name AS organism,
	        cc.rseq_experiment_component_id,
			STRING_AGG(cc.exp_component_name, ';') AS exp_component_name,
	        STRING_AGG(cc.exp_component_vendor_name, ';') AS exp_component_vendor_name,
			STRING_AGG(
			  CASE
			    WHEN cc.experiment_component_failed IS TRUE THEN 'True'
			    WHEN cc.experiment_component_failed IS FALSE THEN 'False'
			  END,
			  ';'
			) AS experiment_component_failed,
	        STRING_AGG(CAST(cc.exp_cluster_density_thousands_per_mm2 AS TEXT), ';') AS exp_cluster_density_thousands_per_mm2,
	        STRING_AGG(CAST(cc.lane_read_count AS TEXT), ';') AS lane_read_count,
	        SUM(cc.vendor_read_count) AS vendor_read_count
		FROM core_cte cc
		JOIN organisms o ON o.id = cc.organism_id_for_alignment 
	    GROUP BY
	        cc.sample_id,
	        cc.rseq_library_prep_id,
	        cc.tube_id,
	       	o.common_name,
	       	cc.rseq_experiment_component_id
	),
	donor_core_metadata_cte AS (
		SELECT DISTINCT
			fwt.id AS facs_well_templates_id,
			-- donors
			d.id AS donor_id,
			d.name AS donor_name,
			d.full_genotype,
			d.external_donor_name,
	        	-- Compute donor sort name once for sorting (external donor name preferred, fallback to d.name)
	        COALESCE(NULLIF(BTRIM(d.external_donor_name), ''), d.name) AS donor_sort_name,
	        LOWER(COALESCE(NULLIF(BTRIM(d.external_donor_name), ''), d.name)) AS ext_donor_sort,
	        -- ages
	        CASE WHEN
	        	a.isembryonic != TRUE AND a.organism_id = (SELECT o.id FROM organisms o WHERE o.common_name = 'mouse') AND a.days > 0
	        	THEN CAST(a.days AS VARCHAR(64)) || ' days'  -- mouse age entries include some dupes represented as weeks
	        	ELSE a.name
	        END AS age,
	        -- cell_prep_samples
	        cps.name AS cell_prep_sample_name,
	        -- genders
	        g.name AS sex,
	        -- organisms
	        o.name AS species,
	        o.common_name AS organism,
	        -- cell_prep_roi_plans
	        cprp.name AS roi,
			-- cell_prep_sample_types
			cpst.name AS cell_prep_type
		FROM sample_ids_cte sic
		JOIN facs_well_templates fwt ON fwt.id = sic.sample_id
		JOIN cell_prep_samples_facs_wells cps_fw ON cps_fw.facs_well_id = fwt.id
	    JOIN cell_prep_samples cps ON cps.id = cps_fw.cell_prep_sample_id
	    JOIN cell_prep_samples_specimens cps_s ON cps_s.cell_prep_sample_id = cps.id
	    JOIN specimens s ON s.id = cps_s.specimen_id
	    JOIN donors d ON d.id = s.donor_id
	    JOIN ages a ON a.id = d.age_id
	    JOIN genders g ON g.id = d.gender_id
	    JOIN organisms o ON o.id = d.organism_id
	    LEFT JOIN cell_prep_roi_plans cprp ON cprp.id = cps.cell_prep_roi_plan_id
		LEFT JOIN cell_prep_sample_types cpst ON cpst.id = cps.cell_prep_sample_type_id
	),
	drivers_cte AS (
		SELECT
			-- One row per donor_id
			dcmc.donor_id,
			STRING_AGG(DISTINCT g.name, ';' ORDER BY g.name) AS name
		FROM donor_core_metadata_cte dcmc
		JOIN donors_genotypes dg ON dg.donor_id = dcmc.donor_id
		JOIN genotypes g ON g.id = dg.genotype_id
		JOIN genotype_types gt ON gt.id = g.genotype_type_id
		WHERE gt.name = 'driver'
		GROUP BY dcmc.donor_id
	),
	reporters_cte AS (
		SELECT
			dcmc.donor_id,
			STRING_AGG(DISTINCT g.name, ';' ORDER BY g.name) AS name
		FROM donor_core_metadata_cte dcmc
		JOIN donors_genotypes dg ON dg.donor_id = dcmc.donor_id
		JOIN genotypes g ON g.id = dg.genotype_id
		JOIN genotype_types gt ON gt.id = g.genotype_type_id
		WHERE gt.name = 'reporter'
		GROUP BY dcmc.donor_id
	),
	medical_conditions_cte AS (
		SELECT
			dcmc.donor_id,
			STRING_AGG(DISTINCT mc.name, ';' ORDER BY mc.name) AS name
		FROM donor_core_metadata_cte dcmc
		JOIN donor_medical_conditions dmc ON dmc.donor_id = dcmc.donor_id
		JOIN medical_conditions mc ON mc.id = dmc.medical_condition_id
		GROUP BY dcmc.donor_id
	),
	-- Use MATERIALIZED here because we only want to compute once and prevent redundant rounds of recursion
	injections_cte(facs_well_templates_id, injection_roi, injection_method, injection_materials) AS MATERIALIZED (
		WITH RECURSIVE specimen_and_ancestors_cte AS (
			-- Base specimen query
			SELECT DISTINCT
				fwt.id AS facs_well_templates_id,
				s.id AS specimen_id,
				0 AS hierarchy_depth
			FROM sample_ids_cte sic -- Use sample_id_cte as base to DRASTICALLY reduce number of specimens to consider
			JOIN facs_well_templates fwt ON fwt.id = sic.sample_id
			JOIN cell_prep_samples_facs_wells cps_fw ON cps_fw.facs_well_id = fwt.id
			JOIN cell_prep_samples cps ON cps.id = cps_fw.cell_prep_sample_id
			JOIN cell_prep_samples_specimens cps_s ON cps_s.cell_prep_sample_id = cps.id
			JOIN specimens s ON s.id = cps_s.specimen_id
			WHERE s.parent_id IS NOT NULL
			UNION ALL
			-- Recursive portion of CTE that grabs specimen ancestors/parents
			SELECT DISTINCT
				saac.facs_well_templates_id,
				s_parent.id AS specimen_id,
				saac.hierarchy_depth + 1 AS hierarchy_depth
			FROM specimen_and_ancestors_cte saac
			JOIN specimens s_parent ON s_parent.id = saac.specimen_id
			WHERE
				s_parent.parent_id IS NOT NULL
				AND saac.hierarchy_depth < 10 -- SAFETY limit to prevent infinite recursion, previous query only went to 8 levels anyways
		),
		-- Map facs_well_id to injection_id(s) via any ancestor specimen
		facs_well_injections_cte AS (
			SELECT
				saac.facs_well_templates_id,
				i_s.injection_id
			FROM specimen_and_ancestors_cte saac
			JOIN injections_specimens i_s ON i_s.specimen_id = saac.specimen_id
		),
		-- Aggregate injection metadata
		injections_metadata_cte AS (
			SELECT
				fwic.facs_well_templates_id,
				s.acronym AS injection_roi,
				i_methods.name AS injection_method,
				i_materials.name AS injection_material
			FROM facs_well_injections_cte fwic
			JOIN injections i ON i.id = fwic.injection_id
			JOIN injection_methods i_methods ON i_methods.id = i.injection_method_id
			JOIN structures s ON s.id = i.targeted_injection_structure_id
			JOIN injection_materials_injections i_materials_i ON i_materials_i.injection_id = i.id
			JOIN injection_materials i_materials ON i_materials.id = i_materials_i.injection_material_id
		),
		-- Aggregate injection metadata per ROI
		injections_by_roi_cte AS (
			SELECT
				imc.facs_well_templates_id,
				imc.injection_roi,
				STRING_AGG(DISTINCT injection_method, ';' ORDER BY injection_method) AS injection_method,
				STRING_AGG(DISTINCT injection_material, ';' ORDER BY injection_material) AS injection_material
			FROM injections_metadata_cte imc
			WHERE
				imc.injection_roi IS NOT NULL
				OR imc.injection_method IS NOT NULL
				OR imc.injection_material IS NOT NULL
			GROUP BY imc.facs_well_templates_id, imc.injection_roi
		)
		-- Final aggregation to one row per facs_well_id
		SELECT
			ibrc.facs_well_templates_id,
			STRING_AGG(DISTINCT ibrc.injection_roi, ';' ORDER BY ibrc.injection_roi) AS injection_roi,
			STRING_AGG(DISTINCT COALESCE(ibrc.injection_method, 'NULL'), ';' ORDER BY COALESCE(ibrc.injection_method, 'NULL')) AS injection_method,
			STRING_AGG(DISTINCT COALESCE(ibrc.injection_material, 'NULL'), ';' ORDER BY COALESCE(ibrc.injection_material, 'NULL')) AS injection_materials
		FROM injections_by_roi_cte ibrc
		GROUP BY ibrc.facs_well_templates_id
	),
	aggregated_donor_metadata_cte AS (
		SELECT
			dcmc.facs_well_templates_id,
	 		STRING_AGG(COALESCE(dcmc.donor_name, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort, dcmc.donor_name) AS donor_name,
	 	    STRING_AGG(COALESCE(dcmc.cell_prep_sample_name, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS cell_prep_sample_name,
			STRING_AGG(DISTINCT COALESCE(dcmc.cell_prep_type, 'NULL'), ';' ORDER BY COALESCE(dcmc.cell_prep_type, 'NULL')) AS cell_prep_type,
	        STRING_AGG(COALESCE(dcmc.full_genotype, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS full_genotype,
	        STRING_AGG(COALESCE(dcmc.external_donor_name, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS external_donor_name,
	        STRING_AGG(COALESCE(dcmc.age, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS age,
	        STRING_AGG(COALESCE(dcmc.sex, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS sex,
	        STRING_AGG(COALESCE(dcmc.species, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS species,
	        STRING_AGG(DISTINCT COALESCE(dcmc.organism, 'NULL'), ';' ORDER BY COALESCE(dcmc.organism, 'NULL')) AS organism,
	        STRING_AGG(DISTINCT COALESCE(dcmc.roi, 'NULL'), ';' ORDER BY COALESCE(dcmc.roi, 'NULL')) AS roi,
	        STRING_AGG(COALESCE(dc.name, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS cre_line,
	        STRING_AGG(COALESCE(rc.name, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS reporter,
	        STRING_AGG(COALESCE(mcc.name, 'NULL'), ';' ORDER BY dcmc.ext_donor_sort) AS medical_conditions,
	        MAX(ic.injection_roi) AS injection_roi,
	        MAX(ic.injection_method) AS injection_method,
	        MAX(ic.injection_materials) AS injection_materials,
	        MIN(dcmc.ext_donor_sort) AS donor_sort_key
		FROM donor_core_metadata_cte dcmc
		LEFT JOIN drivers_cte dc ON dc.donor_id = dcmc.donor_id
		LEFT JOIN reporters_cte rc ON rc.donor_id = dcmc.donor_id
		LEFT JOIN medical_conditions_cte mcc ON mcc.donor_id = dcmc.donor_id
		LEFT JOIN injections_cte ic ON ic.facs_well_templates_id = dcmc.facs_well_templates_id
		GROUP BY dcmc.facs_well_templates_id
	),
	study_list_cte AS (
		SELECT
			-- facs_wells_studies
			fws.facs_well_id,
			-- studies
			ARRAY_TO_STRING(ARRAY_AGG(s.name ORDER BY s.name), ', ') AS studies
		FROM facs_wells_studies fws
		JOIN studies s ON s.id = fws.study_id
		GROUP BY fws.facs_well_id
	),
	sampled_slices_cte AS (
	    -- cell from cell_prep_sample
	    SELECT DISTINCT
	        cell.id AS cell_id,
	        MIN(all_slices.parent_z_coord) AS slice_min_pos,
	        MAX(all_slices.parent_z_coord) AS slice_max_pos
	    FROM sample_ids_cte sic -- Use sample_id_cte as base to DRASTICALLY reduce number of specimens to consider
	    JOIN facs_well_templates fwt ON fwt.id = sic.sample_id
	    JOIN cell_prep_samples cps ON cps.id = fwt.cell_prep_sample_id
	    JOIN cell_prep_samples_specimens cps_s ON cps_s.cell_prep_sample_id = cps.id
	    JOIN specimens hemi_slice ON hemi_slice.id = cps_s.specimen_id
	    JOIN specimens slice ON slice.id = hemi_slice.parent_id
	    JOIN specimens all_slices ON all_slices.parent_id = slice.parent_id
	    JOIN specimens all_hemi_slices ON all_hemi_slices.parent_id = all_slices.id
	    JOIN cell_prep_samples_specimens hemi_cps_s ON hemi_cps_s.specimen_id = all_hemi_slices.id
	    JOIN cell_prep_samples all_samples ON all_samples.id = hemi_cps_s.cell_prep_sample_id
	    JOIN specimens cell ON cell.facs_well_id = fwt.id  -- Added to match cell.id
	    GROUP BY cell.id
	    UNION ALL
	    -- cell from patchseq
	    SELECT DISTINCT
	        cell.id AS cell_id,
	        MIN(all_slices.parent_z_coord) AS slice_min_pos,
	        MAX(all_slices.parent_z_coord) AS slice_max_pos
	    FROM sample_ids_cte sic -- Use sample_id_cte as base to DRASTICALLY reduce number of specimens to consider
	    JOIN specimens cell ON cell.id = sic.sample_id AND cell.patched_cell_container IS NOT NULL
	    JOIN specimens hemi_slice ON hemi_slice.id = cell.parent_id
	    JOIN specimens slice ON slice.id = hemi_slice.parent_id
	    JOIN specimens all_slices ON all_slices.parent_id = slice.parent_id
	    JOIN specimens all_hemi_slices ON all_hemi_slices.parent_id = all_slices.id
	    JOIN specimens all_cells
	        ON all_cells.parent_id = all_hemi_slices.id
	        AND all_cells.patched_cell_container IS NOT NULL
	    WHERE cell.patched_cell_container IS NOT NULL
	    GROUP BY cell.id
	),
	-- Use NOT MATERIALIZED so that WHERE and JOIN filters in main query get pushed down into the CTE. Needs Postgres 12+
	ephys_roi_cte AS NOT MATERIALIZED (
		SELECT DISTINCT
			cell.id AS cell_id,
			s.acronym AS roi_structure
		FROM specimens cell
	    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id
	    JOIN ephys_specimen_roi_plans esrp ON esrp.id = err.ephys_specimen_roi_plan_id
	    JOIN ephys_roi_plans erp ON erp.id = esrp.ephys_roi_plan_id
	    JOIN structures s ON s.id = erp.structure_id
	)
SELECT
	armc.exp_component_name,
	armc.exp_component_vendor_name,
	cc.batch,
	cc.batch_vendor_name,
	cc.tube_set_sent_to_vendor_date,
	cc.tube,
	cc.tube_internal_name,
	cc.tube_contents_nm,
	cc.tube_contents_nm_from_vendor,
	cc.tube_avg_size_bp,
	cc.tube_input_fmol,
	cc.r1_index,
	cc.r2_index,
	cc.index_sequence_pair,
	armc.organism,
	cc.facs_container,
	cc.sample_name,
	cell.patched_cell_container,
	cell.name AS cell_name,
	cell.id AS cell_id,
	slc.studies,
	h."name" AS hemisphere_name,
	cc.sample_quantity_count,
	cc.sample_quantity_pg,
	admc.cell_prep_type,
	admc.donor_name,
	admc.external_donor_name,
	admc."age",
	admc.species,
	admc.sex,
	cc.control,
	admc.full_genotype,
	cc.facs_population_plan,
	admc.cre_line,
	admc.reporter,
	admc.cell_prep_sample_name,
	admc.injection_roi,
	admc.injection_method,
	admc.injection_materials,
	admc.roi,
	erc.roi_structure AS patchseq_roi,
	admc.medical_conditions,
	ssc.slice_min_pos,
	ssc.slice_max_pos,
	cc.rna_amplification_set,
	cc.rna_amplification,
	cc."method",
	cc.amp_date,
	cc.pcr_cycles,
	cc.percent_cdna_longer_than_400bp,
	cc.rna_amplification_pass_fail,
	cc.amplified_quantity_ng,
	cc.load_name,
	cc.port_well,
	cc.library_prep_set,
	cc.library_prep,
	cc.lib_method,
	cc.lib_date,
	cc.library_input_ng,
	cc.avg_size_bp,
	cc.quantification2_ng,
	cc.quantification_fmol,
	cc.quantification2_nm,
	cc.library_prep_pass_fail,
	cc.expc_cell_capture,
	armc.exp_cluster_density_thousands_per_mm2,
	armc.lane_read_count,
	armc.vendor_read_count,
	CASE
	  WHEN cc.experiment_component_failed IS TRUE THEN 'True'
	  WHEN cc.experiment_component_failed IS FALSE THEN 'False'
	END AS experiment_component_failed
FROM core_cte cc
JOIN analysis_run_metadata_cte armc ON armc.rseq_experiment_component_id = cc.rseq_experiment_component_id
LEFT JOIN aggregated_donor_metadata_cte admc ON admc.facs_well_templates_id = cc.sample_id
LEFT JOIN specimens cell ON cell.facs_well_id = cc.facs_well_templates_id OR (cell.id = cc.sample_id AND cell.patched_cell_container IS NOT NULL)
LEFT JOIN hemispheres h ON h.id = cell.hemisphere_id
LEFT JOIN sampled_slices_cte ssc ON ssc.cell_id = cell.id
LEFT JOIN study_list_cte slc ON slc.facs_well_id = cc.facs_well_templates_id
LEFT JOIN ephys_roi_cte erc ON erc.cell_id = cell.id
ORDER BY cc.exp_component_vendor_name;
