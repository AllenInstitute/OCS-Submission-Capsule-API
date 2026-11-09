# Alignment commands

Modality | Command | Asset | Tag | Library Prep Methods | Input Identifier | Distinguishing Options
--- | --- | --- | --- | --- | --- | ---
MTX | ocs fastqs align tenx-arc | cellranger-arc | 2.2.0 | 10xMultX_GEX | Load name | --cellranger-addopts --create-bam=true
MTX | ocs fastqs align tenx-arc | cellranger-arc | Default | 10xRSeq_Mult | Load name | 
RTX | ocs fastqs align tenx-rnaseq | cellranger-rnaseq | latest | 10xV3.1D; 10xRseq_Mult_noATAC; 10xV3.1_HT; 10Xv3.1; 10Xv3.1_LT; 10Xv3; 10Xv2 | Load name | --cellranger-addopts --chemistry {chemistry} --include-introns<br>Chemistry: 10xV3.1D: SC3Pv3; 10xRseq_Mult_noATAC: ARC-v1; 10xV3.1_HT: SC3Pv3HT; 10Xv3.1: SC3Pv3; 10Xv3.1_LT: SC3Pv3LT; 10Xv3: SC3Pv3; 10Xv2: SC3Pv2
RTX | ocs fastqs align tenx-rnaseq-multi | cellranger-multi | latest | 10xV3.1_HT_CP | Load name | --cellranger-addopts --include-introns
RTX | ocs fastqs align tenx-rnaseq-multi | cellranger-multi | 9.0.1 | 10xV4_PX | Load name | --cellranger-addopts --create-bam=TRUE
RTX | ocs fastqs align tenx-rnaseq | cellranger-rnaseq | 8.0.1 | 10xV4; 10xV4-TK | Load name | --cellranger-addopts --chemistry {chemistry}<br>Chemistry: 10xV4: SC3Pv4; 10xV4-TK: SC3Pv4
RFX | ocs fastqs align tenx-rnaseq-multi | cellranger-multi | 10.1.0 | 10xFXv2; 10xV3.1_FX; 10xV4_FX16; 10xV4_FX16-TK; 10xV4_FX4; 10xV4_FX4-TK; 10xV4_FX | Load name | --cellflex-probe-set-name {probe_set}<br>--cellranger-addopts --create-bam=true<br>--execution-vcpus 128<br>Organisms: mouse; human
