with sampled_slices(cell_id, slice_min_pos, slice_max_pos) as (
    --cell from cell_prep_sample
    select distinct 
        cell.id, 
        min(all_slices.parent_z_coord), 
        max(all_slices.parent_z_coord) 
    from specimens cell
    join facs_well_templates fw on fw.id = cell.facs_well_id
    join cell_prep_samples cps on cps.id = fw.cell_prep_sample_id
    join cell_prep_samples_specimens cps2sp on cps2sp.cell_prep_sample_id = cps.id
    join specimens hemi_slice on hemi_slice.id = cps2sp.specimen_id
    join specimens slice on slice.id = hemi_slice.parent_id
    join specimens all_slices on all_slices.parent_id = slice.parent_id
    join specimens all_hemi_slices on all_hemi_slices.parent_id = all_slices.id
    join cell_prep_samples_specimens cps2sp2 on cps2sp2.specimen_id = all_hemi_slices.id
    join cell_prep_samples all_samples on all_samples.id = cps2sp2.cell_prep_sample_id
    group by cell.id
    UNION
    --cell from patchseq
    select distinct 
        cell.id, 
        min(all_slices.parent_z_coord), 
        max(all_slices.parent_z_coord) 
    from specimens cell
    join specimens hemi_slice on hemi_slice.id = cell.parent_id
    join specimens slice on slice.id = hemi_slice.parent_id
    join specimens all_slices on all_slices.parent_id = slice.parent_id
    join specimens all_hemi_slices on all_hemi_slices.parent_id = all_slices.id
    join specimens all_cells on all_cells.parent_id = all_hemi_slices.id and all_cells.patched_cell_container is not null
    where cell.patched_cell_container is not null
    group by cell.id
),

study_list(facs_well_id, studies) as (
    select 
        fwstdy.facs_well_id, 
        array_to_string(array_agg(stdy.name order by stdy.name), ', ') as studies
    from facs_wells_studies fwstdy
    join studies stdy on fwstdy.study_id = stdy.id
    group by fwstdy.facs_well_id
),

-- donors are aggregated at the sample level, all associated
-- columns need to be aggregated as well and sorted by donor
-- to match each other
donor_list (
    fw_id, 
    donor_name,
    cell_prep_sample_name,
    full_genotype, 
    external_donor_name, 
    age, 
    sex, 
    species, 
    organism, 
	roi,
    cre_line, 
    reporter, 
    medical_conditions,
    injection_roi,
    injection_method,
    injection_materials
) as (
    with donor_items (
        fw_id, 
        donor_id, 
        donor, 
        cell_prep_sample_name,
        full_genotype, 
        external_donor_name, 
        age, 
        sex, 
        species, 
        organism,
        ext_donor_sort,
        roi
    ) as (
    select
        fw.id,
        d.id,
        d.name,
        cps.name,
        d.full_genotype,
        d.external_donor_name,
        case when a.isembryonic != true and a.organism_id = 2 and a.days > 0 
            then cast(a.days as varchar(64)) || ' days'-- mouse age entries include some dupes represented as weeks
            else a.name 
        end as age,
        case when d.gender_id=1 then 'M' 
            when d.gender_id=2 then 'F' 
            when d.gender_id=3 then 'unknown' 
        end as sex,
        org.name,
        org.common_name,
        -- alphanumeric sort key
		lower(coalesce(nullif(d.external_donor_name,''), d.name)) as ext_donor_sort,
        cproi.name as roi
    from facs_well_templates fw
    join cell_prep_samples_facs_wells cps2fw on cps2fw.facs_well_id = fw.id
    join cell_prep_samples cps on cps.id = cps2fw.cell_prep_sample_id
    join cell_prep_samples_specimens cps2sp on cps2sp.cell_prep_sample_id = cps.id
    join specimens sp on sp.id = cps2sp.specimen_id
    join donors d on d.id = sp.donor_id
    join ages a on a.id=d.age_id
    join organisms org on org.id = d.organism_id
    left join cell_prep_roi_plans cproi on cproi.id = cps.cell_prep_roi_plan_id
    group by 
        fw.id, 
        d.id, 
        d.name, 
        cps.name,
        d.full_genotype, 
        d.external_donor_name, 
        a.isembryonic, 
        a.organism_id, 
        a.days, 
        a.name, 
        d.gender_id,
        org.name,
        org.common_name,
        lower(coalesce(nullif(d.external_donor_name,''), d.name)),
		cproi.name
    ),
    inj_table(donor_id, inj_roi, inj_method, inj_materials) as (
        with inj_pre_agg(donor_id, inj_roi, inj_method, inj_materials) as (
                -- join injection to facs_well
                select distinct d.id as donor_id, st.acronym as inj_roi, injm.name as inj_method, inj_mat.name as inj_materials
                    from donors d
                    join specimens sp on sp.donor_id = d.id
                    join cell_prep_samples_specimens cps2sp on cps2sp.specimen_id = sp.id
                    join cell_prep_samples cps on cps.id = cps2sp.cell_prep_sample_id
                    join cell_prep_samples_facs_wells cps2fw on cps2fw.cell_prep_sample_id = cps.id
                    join facs_well_templates fw on fw.id = cps2fw.facs_well_id
                    join facs_wells_injection_materials fw2im on fw2im.facs_well_id = fw.id
                    join injection_materials inj_mat on inj_mat.id = fw2im.injection_material_id
                    join injection_materials_injections inj2mat on inj2mat.injection_material_id = inj_mat.id
                    join injections inj on inj.id = inj2mat.injection_id
                    join injection_methods injm on injm.id = inj.injection_method_id
                    join structures st on st.id = inj.targeted_injection_structure_id
                UNION
                -- join injection to specimen
                select distinct d.id as donor_id, st.acronym as inj_roi, injm.name as inj_method, inj_mat.name as inj_materials
                    from donors d
                    join specimens sp on sp.donor_id = d.id
                    join cell_prep_samples_specimens cps2sp on cps2sp.specimen_id = sp.id
                    join cell_prep_samples cps on cps.id = cps2sp.cell_prep_sample_id
                    join injections_specimens inj2sp on inj2sp.specimen_id = sp.id
                    join injections inj on inj.id = inj2sp.injection_id
                    join injection_materials_injections inj2mat on inj2mat.injection_id = inj.id
                    join injection_materials inj_mat on inj_mat.id = inj2mat.injection_material_id
                    join injection_methods injm on injm.id = inj.injection_method_id
                    join structures st on st.id = inj.targeted_injection_structure_id
        )
        select distinct donor_id, inj_roi, inj_method, string_agg(inj_materials, '_and_' ORDER BY inj_materials) as inj_materials
        from inj_pre_agg
        group by donor_id, inj_roi, inj_method
    ),
    drivers(donor_id, name) as (
    with drivers_table(donor_id, name) as (
        select distinct d.id, g.name
        from donors d
        join donors_genotypes d2g on d2g.donor_id = d.id
        join genotypes g on g.id = d2g.genotype_id
        join genotype_types gt on gt.id = g.genotype_type_id
    	where gt.name = 'driver'
        order by d.id, g.name
    )
    select distinct donor_id as donor_id, string_agg(name, '_and_') as name 
    from drivers_table
    group by donor_id
    ),
    reporters(donor_id, name) as (
        select distinct d.id, g.name
        from donors d
        join donors_genotypes d2g on d2g.donor_id = d.id
        join genotypes g on g.id = d2g.genotype_id
        join genotype_types gt on gt.id = g.genotype_type_id
        where gt.name = 'reporter'
    ),
    med_conditions(donor_id, name) as (
        with medcon_table(donor_id, name) as (
            select distinct d.id, medcon.name
            from donors d
            join donor_medical_conditions d2medcon on d2medcon.donor_id = d.id
            join medical_conditions medcon on medcon.id = d2medcon.medical_condition_id
            order by d.id, medcon.name
        )
		select distinct donor_id as donor_id, string_agg(name, '_and_') as name 
		from medcon_table
		group by donor_id
    )
    select 
        di.fw_id, 
        string_agg(coalesce(di.donor, 'NULL'), ';' order by di.ext_donor_sort, di.donor) as donor_name,
        string_agg(coalesce(di.cell_prep_sample_name, 'NULL'), ';' order by di.ext_donor_sort) as cell_prep_sample_name,
        string_agg(coalesce(di.full_genotype, 'NULL'), ';' order by di.ext_donor_sort) as full_genotype,
        string_agg(coalesce(di.external_donor_name, 'NULL'), ';' order by di.ext_donor_sort) as external_donor_name,
        string_agg(coalesce(di.age, 'NULL'), ';' order by di.ext_donor_sort) as age,
        string_agg(coalesce(di.sex, 'NULL'), ';' order by di.ext_donor_sort) as sex,
        string_agg(coalesce(di.species, 'NULL'), ';' order by di.ext_donor_sort) as species,
        string_agg(coalesce(di.organism, 'NULL'), ';' order by di.ext_donor_sort) as organism,
        string_agg(coalesce(di.roi,'NULL'), ';' order by di.ext_donor_sort) as roi,
        string_agg(coalesce(dr.name, 'NULL'), ';' order by di.ext_donor_sort) as cre_line,
        string_agg(coalesce(rp.name, 'NULL'), ';' order by di.ext_donor_sort) as reporter,
        string_agg(coalesce(mc.name, 'NULL'), ';' order by di.ext_donor_sort) as medical_conditions,
        string_agg(coalesce(inj.inj_roi, 'NULL'), ';' order by lower(inj.inj_roi), lower(inj.inj_method)) as injection_roi,
        string_agg(coalesce(inj.inj_method, 'NULL'), ';' order by lower(inj.inj_roi), lower(inj.inj_method)) as injection_method,
        string_agg(coalesce(inj.inj_materials, 'NULL'), ';' order by lower(inj.inj_roi), lower(inj.inj_method)) as injection_materials
    from donor_items di
    left join drivers dr on dr.donor_id = di.donor_id
    left join reporters rp on rp.donor_id = di.donor_id
    left join med_conditions mc on mc.donor_id = di.donor_id
    left join inj_table inj on inj.donor_id = di.donor_id
    group by fw_id
),

cell_reporters(cell_id, pos_or_neg) as (
  select distinct sp.id as cell_id, cr.name as name 
  from specimens sp
  join cell_reporters cr on cr.id = sp.cell_reporter_id
),

ephys_roi(cell_id, roi_structure) as (
    select distinct cell.id, st.acronym 
    from specimens cell
    join ephys_roi_results err on err.id = cell.ephys_roi_result_id
    join ephys_specimen_roi_plans esrp on esrp.id = err.ephys_specimen_roi_plan_id --and esrp.specimen_id = cell.id
    join ephys_roi_plans erp on erp.id = esrp.ephys_roi_plan_id
    join structures st on st.id = erp.structure_id
    order by cell.id
),

ar_metadata (
    sample_id, 
    rseq_library_prep_id, 
    tube_id, 
    expc_name, 
    expc_name_from_vendor, 
    expc_failed, 
    exp_cluster_density_thousands_per_mm2, 
    lane_read_count, 
    vendor_read_count, 
    organism_id_for_alignment, 
    expc_id
) as (
    with ar_related (
        sample_id, 
        rseq_library_prep_id, 
        tube_id, 
        expc_name, 
        expc_name_from_vendor, 
        expc_failed, 
        exp_cluster_density_thousands_per_mm2, 
        exp_vendor_read_count, 
        expc_vendor_read_count, 
        organism_id_for_alignment, 
        expc_id
    ) as (
        select distinct 
            expc.sample_id, 
            expc.rseq_library_prep_id, 
            t.id, 
            expc.name, 
            expc.name_from_vendor, 
            expc.failed, 
            exp.cluster_density_thousands_per_mm2, 
            exp.vendor_read_count, 
            expc.vendor_read_count, 
            expc.organism_id_for_alignment, 
            expc.id
        from rseq_experiment_components expc
        join rseq_experiments exp on exp.id = expc.rseq_experiment_id
        join rseq_tubes t on t.id = exp.rseq_tube_id
        order by expc.name
    )
    select 
        arr.sample_id, 
        arr.rseq_library_prep_id, 
        arr.tube_id, 
        string_agg(arr.expc_name, '_and_'), 
        string_agg(arr.expc_name_from_vendor, '_and_'), 
        string_agg(arr.expc_failed::text, '_and_'),  
        string_agg(arr.exp_cluster_density_thousands_per_mm2::text, '_and_'),
        string_agg(arr.exp_vendor_read_count::text, '_and_'),  
        sum(arr.expc_vendor_read_count), 
        arr.organism_id_for_alignment, 
        arr.expc_id
    from ar_related arr
    group by 
        arr.sample_id, 
        arr.rseq_library_prep_id, 
        arr.tube_id, 
        arr.organism_id_for_alignment, 
        arr.expc_id
),

fastq_files(expc_id, fastq_path_directory, fastq_R1_list) as (
    with fastq_table(expc_id, fastq_path_directory, expected_fastq_I1_path) as (
        select distinct 
			expc.id, 
			ts.storage_directory  || 
				ts.name_from_vendor || 
				'_' || 
				ts.name ||
				'/RAW-DATA/' || 
				t.name || 
				'/',
			ts.storage_directory  || 
				ts.name_from_vendor || 
				'_' || 
				ts.name ||
				'/RAW-DATA/' || 
				t.name || 
				'/' || 
				exp.name || 
				'_' || 
				'S01_L003_I1_001.fastq.gz' 
        from rseq_experiment_components expc
        join rseq_experiments exp on exp.id = expc.rseq_experiment_id
        join rseq_tubes t on t.id = exp.rseq_tube_id
        join rseq_tube_sets ts on ts.id = t.rseq_tube_set_id
        order by 1,2 
    )
    select 
        fastq_table.expc_id as expc_id, 
        fastq_path_directory, 
        string_agg(expected_fastq_I1_path, '_and_') as fastq_R1_list 
    from fastq_table
    group by fastq_table.expc_id, fastq_table.fastq_path_directory
)

select distinct
	armd.expc_name as exp_component_name, 
	armd.expc_name_from_vendor as exp_component_vendor_name, 
	ts.name as batch,
	ts.name_from_vendor as batch_vendor_name, 
	t.name as tube, 
	t.internal_name as tube_internal_name,
	t.content_concentration_nm as tube_contents_nm,
	t.concentration_from_vendor_nm as tube_contents_nm_from_vendor,
	t.avg_size_bp as tube_avg_size_bp,
	ti.input_quantity_fmol as tube_input_fmol,
	r1index.name as r1_index,
	r2index.name as r2_index,
	r1seq.sequence_data || '-' || r2seq.sequence_data as index_sequence_pair,
	d.organism,
	fp.name as facs_container, 
	fw.name as sample_name,
	cell.patched_cell_container,
	cell.name as cell_name,
	cell.id as cell_id,
	stdy.studies,
	hem.name as hemisphere_name,
	rai.sample_quantity_count,
	rai.sample_quantity_pg,
	d.donor_name,
	d.external_donor_name,
	d.age,
	d.species,
	d.sex,
	ctl.name as control,
	d.full_genotype,
	fpop.name as facs_population_plan,
	d.cre_line,
	d.reporter,
    d.cell_prep_sample_name,
	d.injection_roi, 
	d.injection_method,
	d.injection_materials,
	d.roi,
	ephys_roi.roi_structure as patchseq_roi,
	d.medical_conditions,
	ssl.slice_min_pos,
	ssl.slice_max_pos,
	ras.name as rna_amplification_set,
	ra.name as rna_amplification,
	ram.name as method,
	null as amp_date,
	ra.cycles as pcr_cycles,
	ra.percent_cdna_longer_than_400bp,
	case ra.failed when 'f' then 'Pass' when 't' then 'Fail' end as rna_amplification_pass_fail,
	ra.amplified_quantity_ng,
    ra.load_name as load_name,
	ra.port_well as port_well,
	lps.name as library_prep_set,
	lpm.name as lib_method,
	lp.run_date as lib_date,
	lp.input_quantity_ng as library_input_ng,
	lp.avg_size_bp,
	lp.quantification2_ng,
	lp.quantification_fmol,
	lp.quantification2_nm,
	case lp.fail when 'f' then 'Pass' when 't' then 'Fail' end as library_prep_pass_fail,
	armd.exp_cluster_density_thousands_per_mm2,
	armd.lane_read_count,
	armd.vendor_read_count,
	armd.expc_failed as experiment_component_failed,
	fastq_files.fastq_path_directory,
	fastq_files.fastq_R1_list as expected_I1_fastq,
    lower(coalesce(nullif(d.external_donor_name,''), d.donor_name)) as donor_sort_key

from rseq_tubes t
left join ar_metadata armd on armd.tube_id=t.id
join rseq_tube_sets ts on ts.id = t.rseq_tube_set_id
join projects p on p.id = ts.project_id
left join fastq_files on fastq_files.expc_id = armd.expc_id
left join facs_well_templates fw on fw.id = armd.sample_id
left join facs_population_plans fpop on fpop.id = fw.facs_population_plan_id
left join specimens cell on cell.facs_well_id = fw.id or (cell.id = armd.sample_id and cell.patched_cell_container is not null)

left join hemispheres hem on hem.id = cell.hemisphere_id
left join cell_reporters cellrep on cellrep.cell_id = cell.id
left join external_controls ctl on ctl.id = fw.external_control_id
left join facs_plate_templates fp on fp.id = fw.facs_plate_id
left join cell_prep_samples_facs_wells cps2fw on cps2fw.facs_well_id = fw.id
left join cell_prep_samples cps on cps.id = cps2fw.cell_prep_sample_id
left join cell_prep_sample_types cpst on cpst.id = cps.cell_prep_sample_type_id
left join cell_prep_roi_plans cproi on cproi.id = cps.cell_prep_roi_plan_id
left join sampled_slices ssl on ssl.cell_id = cell.id

-- studies associated with  facs_wells
left join study_list stdy on fw.id = stdy.facs_well_id
left join donor_list d on d.fw_id = fw.id
left join ephys_roi on ephys_roi.cell_id = cell.id
join rna_amplification_inputs rai on (rai.sample_id = fw.id or rai.sample_id = cell.id)
join rna_amplifications ra on ra.id = rai.rna_amplification_id
join rna_amplification_methods ram on ram.id = ra.rna_amplification_method_id
join rseq_library_preps lp on lp.input_id = ra.id and lp.id = armd.rseq_library_prep_id
join rseq_library_prep_sets lps on lps.id = lp.rseq_library_prep_set_id
join rseq_library_prep_methods lpm on lpm.id = lp.rseq_library_prep_method_id
left join rseq_tube_inputs ti on ti.input_id = lp.id and ti.rseq_tube_id = t.id
left join rseq_oligos r1index on r1index.id = lp.read1_index_id
left join sequences r1seq on r1seq.id = r1index.sequence_id
left join rseq_oligos r2index on r2index.id = lp.read2_index_id
left join sequences r2seq on r2seq.id = r2index.sequence_id
join rna_amplification_sets ras on ras.id = ra.rna_amplification_set_id
where ts.name_from_vendor = any(array[{batch_name}]) or armd.expc_name = any(array[{exp_component_name}]) or ra.load_name = any(array[{load_name}])
order by armd.expc_name_from_vendor, donor_sort_key;