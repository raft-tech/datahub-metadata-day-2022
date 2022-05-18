import React from "react";
import clsx from "clsx";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";

import styles from "../styles/logos.module.scss";

const companiesByIndustry = [
  {
    name: "B2B & B2C",
    companies: [
      {
        name: "LinkedIn",
        imageUrl: "/img/logos/companies/linkedin.svg",
        size: "small",
      },
      {
        name: "Udemy",
        imageUrl: "/img/logos/companies/udemy.png",
        size: "defualt",
      },
      {
        name: "Geotab",
        imageUrl: "/img/logos/companies/geotab.jpg",
        size: "small",
      },
      {
        name: "ThoughtWorks",
        imageUrl: "/img/logos/companies/thoughtworks.png",
        size: "default",
      },
      {
        name: "Expedia Group",
        imageUrl: "/img/logos/companies/expedia.svg",
        size: "default",
      },
      {
        name: "Typeform",
        imageUrl: "/img/logos/companies/typeform.svg",
        size: "small",
      },
      {
        name: "Peloton",
        imageUrl: "/img/logos/companies/peloton.png",
        size: "large",
      },
      {
        name: "Zynga",
        imageUrl: "/img/logos/companies/zynga.png",
        size: "default",
      },
    ],
  },
  {
    name: "Financial & Fintech",
    companies: [
      {
        name: "Saxo Bank",
        imageUrl: "/img/logos/companies/saxobank.svg",
        size: "default",
      },
      {
        name: "Klarna",
        imageUrl: "/img/logos/companies/klarna.svg",
        size: "small",
      },
      {
        name: "BankSalad",
        imageUrl: "/img/logos/companies/banksalad.png",
        size: "large",
      },
      {
        name: "Uphold",
        imageUrl: "/img/logos/companies/uphold.png",
        size: "large",
      },
      {
        name: "Stash",
        imageUrl: "/img/logos/companies/stash.svg",
        size: "large",
      },
      {
        name: "SumUp",
        imageUrl: "/img/logos/companies/sumup.png",
        size: "large",
      },
    ],
  },
  {
    name: "E-Commerce",
    companies: [
      {
        name: "Adevinta",
        imageUrl: "/img/logos/companies/adevinta.png",
        size: "default",
      },      {
        name: "Grofers",
        imageUrl: "/img/logos/companies/grofers.png",
        size: "default",
      },
      {
        name: "SpotHero",
        imageUrl: "/img/logos/companies/spothero.png",
        size: "default",
      },
      {
        name: "hipages",
        imageUrl: "/img/logos/companies/hipages.png",
        size: "default",
      },
      {
        name: "Wolt",
        imageUrl: "/img/logos/companies/wolt.png",
        size: "large",
      },
    ],
  },
  {
    name: "And More",
    companies: [
      {
        name: "Cabify",
        imageUrl: "/img/logos/companies/cabify.png",
        size: "large",
      },
      {
        name: "Viasat",
        imageUrl: "/img/logos/companies/viasat.png",
        size: "large",
      },
      {
        name: "DFDS",
        imageUrl: "/img/logos/companies/dfds.png",
        size: "large",
      },
      {
        name: "Moloco",
        imageUrl: "/img/logos/companies/moloco.png",
        size: "default",
      },
      {
        name: "Optum",
        imageUrl: "/img/logos/companies/optum.jpg",
        size: "large",
      },
    ],
  },
];

const platformLogos = [
  {
    name: "ADLS",
    imageUrl: "/img/logos/platforms/adls.svg",
  },
  {
    name: "Airflow",
    imageUrl: "/img/logos/platforms/airflow.svg",
  },
  {
    name: "Athena",
    imageUrl: "/img/logos/platforms/athena.svg",
  },
  {
    name: "BigQuery",
    imageUrl: "/img/logos/platforms/bigquery.svg",
  },
  {
    name: "CouchBase",
    imageUrl: "/img/logos/platforms/couchbase.svg",
  },
  { name: "DBT", imageUrl: "/img/logos/platforms/dbt.svg" },
  { name: "Druid", imageUrl: "/img/logos/platforms/druid.svg" },
  { name: "Elasticsearch", imageUrl: "/img/logos/platforms/elasticsearch.svg" },
  {
    name: "Feast",
    imageUrl: "/img/logos/platforms/feast.svg",
  },
  {
    name: "Glue",
    imageUrl: "/img/logos/platforms/glue.svg",
  },
  {
    name: "Hadoop",
    imageUrl: "/img/logos/platforms/hadoop.svg",
  },
  {
    name: "Hive",
    imageUrl: "/img/logos/platforms/hive.svg",
  },
  { name: "Kafka", imageUrl: "/img/logos/platforms/kafka.svg" },
  { name: "Kusto", imageUrl: "/img/logos/platforms/kusto.svg" },
  { name: "Looker", imageUrl: "/img/logos/platforms/looker.svg" },
  { name: "Metabase", imageUrl: "/img/logos/platforms/metabase.svg" },
  { name: "Mode", imageUrl: "/img/logos/platforms/mode.png" },
  { name: "MongoDB", imageUrl: "/img/logos/platforms/mongodb.svg" },
  {
    name: "MSSQL",
    imageUrl: "/img/logos/platforms/mssql.svg",
  },
  {
    name: "MySQL",
    imageUrl: "/img/logos/platforms/mysql.svg",
  },
  { name: "Nifi", imageUrl: "/img/logos/platforms/nifi.svg" },
  { name: "Oracle", imageUrl: "/img/logos/platforms/oracle.svg" },
  { name: "Pinot", imageUrl: "/img/logos/platforms/pinot.svg" },
  { name: "PostgreSQL", imageUrl: "/img/logos/platforms/postgres.svg" },
  { name: "PowerBI", imageUrl: "/img/logos/platforms/powerbi.png" },
  { name: "Presto", imageUrl: "/img/logos/platforms/presto.svg" },
  { name: "Redash", imageUrl: "/img/logos/platforms/redash.svg" },
  {
    name: "Redshift",
    imageUrl: "/img/logos/platforms/redshift.svg",
  },
  {
    name: "S3",
    imageUrl: "/img/logos/platforms/s3.svg",
  },
  {
    name: "SageMaker",
    imageUrl: "/img/logos/platforms/sagemaker.svg",
  },
  { name: "Snowflake", imageUrl: "/img/logos/platforms/snowflake.svg" },
  { name: "Spark", imageUrl: "/img/logos/platforms/spark.svg" },
  {
    name: "Superset",
    imageUrl: "/img/logos/platforms/superset.svg",
  },
  {
    name: "Tableau",
    imageUrl: "/img/logos/platforms/tableau.png",
  },
  {
    name: "Teradata",
    imageUrl: "/img/logos/platforms/teradata.svg",
  },
];

export const PlatformLogos = () => (
  <Link to={useBaseUrl("docs/metadata-ingestion#installing-plugins/")} className={styles.marquee}>
    <div>
      {[...platformLogos, ...platformLogos].map((logo, idx) => (
        <img src={useBaseUrl(logo.imageUrl)} alt={logo.name} title={logo.name} key={idx} className={styles.platformLogo} />
      ))}
    </div>
  </Link>
);

export const CompanyLogos = () => (
  <div className={clsx("container", styles.companyLogoContainer)}>
    <Tabs className="pillTabs">
      {companiesByIndustry.map((industry, idx) => (
        <TabItem value={`industry-${idx}`} label={industry.name} key={idx} default={idx === 0}>
          <div className={styles.companyWrapper}>
            {industry.companies.map((company, idx) => (
              <img
                src={useBaseUrl(company.imageUrl)}
                alt={company.name}
                title={company.name}
                key={idx}
                className={clsx(styles.companyLogo, styles[company.size])}
              />
            ))}
          </div>
        </TabItem>
      ))}
    </Tabs>
  </div>
);
