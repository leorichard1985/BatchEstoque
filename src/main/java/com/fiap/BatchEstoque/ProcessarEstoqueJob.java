package com.fiap.BatchEstoque;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class ProcessarEstoqueJob implements FieldSetMapper<CatalogoProduto> {

	private static int CHUNK_SIZE = 10000;

	private DataSource datasource;

	public ProcessarEstoqueJob(DataSource datasource) {
		this.datasource = datasource;
	}

	@Bean
	public Job jobProcessarEstoqueProduto(Step stepProcessarEstoqueProduto, Step stepProcessarCarga,
			JobRepository jobRepository) {

		return new JobBuilder("jobProcessarEstoqueProduto", jobRepository).start(stepProcessarEstoqueProduto)
				.next(stepProcessarCarga).incrementer(new RunIdIncrementer()).build();

	}

	@Bean
	public Step stepProcessarEstoqueProduto(JobRepository jobRepository,
			DataSourceTransactionManager transactionManager, FlatFileItemReader<CatalogoProduto> reader,
			ItemWriter<CatalogoProduto> writer) {

		return new StepBuilder("stepProcessarEstoqueProduto", jobRepository)
				.<CatalogoProduto, CatalogoProduto>chunk(CHUNK_SIZE, transactionManager).reader(reader).writer(writer)
				.faultTolerant().skipLimit(5).skip(PessimisticLockingFailureException.class).build();

	}

	@Bean
	public Step stepProcessarCarga(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
			Tasklet tasklet) {

		return new StepBuilder("stepProcessarCarga", jobRepository).tasklet(tasklet, transactionManager).build();

	}

	@Bean
	public Tasklet tasklet() {
		return (contribution, chunkContext) -> {

			SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(datasource);

			simpleJdbcCall.withProcedureName("spu_ProcessarProdutos");
			simpleJdbcCall.withSchemaName("dbo");
			simpleJdbcCall.execute();

			return RepeatStatus.FINISHED;
		};
	}

	@Bean
	public FlatFileItemReader<CatalogoProduto> reader() {
		return new FlatFileItemReaderBuilder<CatalogoProduto>().name("reader")
				.resource(new ClassPathResource("catalagoProduto.txt")).lineMapper(catalogoLineMapper()).strict(false)
				.build();
	}

	@Bean
	public LineMapper<CatalogoProduto> catalogoLineMapper() {

		DefaultLineMapper<CatalogoProduto> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();

		tokenizer.setDelimiter("|");
		tokenizer.setNames(new String[] { "id_Produto", "nome_Produto", "qtde_Disponivel" });

		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(this);

		return lineMapper;

	}

	@Override
	public CatalogoProduto mapFieldSet(FieldSet fieldSet) {
		CatalogoProduto catProduto = new CatalogoProduto();

		catProduto.setId_Produto(fieldSet.readInt("id_Produto"));
		catProduto.setNome_Produto(fieldSet.readString("nome_Produto"));
		catProduto.setQtde_Disponivel(fieldSet.readInt("qtde_Disponivel"));

		return catProduto;
	}

	@Bean
	public ItemWriter<CatalogoProduto> write(DataSource datasource) {
		return new JdbcBatchItemWriterBuilder<CatalogoProduto>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.dataSource(datasource)
				.sql("INSERT INTO tb_TMP_ProdEstoque" + "(" + "id_Produto, " + "nome_Produto, " + "qtde_Disponivel"
						+ ") " + " VALUES " + "(" + ":id_Produto, " + ":nome_Produto, " + ":qtde_Disponivel" + ")")
				.build();
	}

}
