-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema imdb
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema imdb
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `imdb` DEFAULT CHARACTER SET utf8 ;
USE `imdb` ;

-- -----------------------------------------------------
-- Table `imdb`.`name_basics`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`name_basics` ;

CREATE TABLE IF NOT EXISTS `imdb`.`name_basics` (
  `nconst` INT NOT NULL,
  `primaryName` VARCHAR(110) NULL,
  `birthYear` INT NULL,
  `deathYear` INT NULL,
  `primaryProfession` VARCHAR(70) NULL,
  `knownForTitles` VARCHAR(70) NULL,
  PRIMARY KEY (`nconst`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `imdb`.`title_basics`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`title_basics` ;

CREATE TABLE IF NOT EXISTS `imdb`.`title_basics` (
  `tconst` INT NOT NULL,
  `titleType` VARCHAR(45) NULL,
  `primaryTitle` VARCHAR(410) NULL,
  `originalTitle` VARCHAR(410) NULL,
  `isAdult` VARCHAR(45) NULL,
  `startYear` VARCHAR(45) NULL,
  `endYear` VARCHAR(45) NULL,
  `runtimeMinutes` VARCHAR(45) NULL,
  `genres` VARCHAR(45) NULL,
  PRIMARY KEY (`tconst`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `imdb`.`title_akas`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`title_akas` ;

CREATE TABLE IF NOT EXISTS `imdb`.`title_akas` (
  `titleid` INT NOT NULL,
  `ordering` INT NULL,
  `title` VARCHAR(832) NULL,
  `region` VARCHAR(45) NULL,
  `language` VARCHAR(45) NULL,
  `types` VARCHAR(45) NULL,
  `attributes` VARCHAR(62) NULL,
  `isOriginalTitle` INT NULL,
  INDEX `fk_title_akas_title_basics_idx` (`titleid` ASC),
  CONSTRAINT `fk_title_akas_title_basics`
    FOREIGN KEY (`titleid`)
    REFERENCES `imdb`.`title_basics` (`tconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `imdb`.`title_crew_director`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`title_crew_director` ;

CREATE TABLE IF NOT EXISTS `imdb`.`title_crew_director` (
  `tconst` INT NOT NULL,
  `director` INT NOT NULL,
  INDEX `fk_title_crew_title_basics1_idx` (`tconst` ASC),
  INDEX `fk_title_crew_name_basics1_idx` (`director` ASC),
  CONSTRAINT `fk_title_crew_title_basics1`
    FOREIGN KEY (`tconst`)
    REFERENCES `imdb`.`title_basics` (`tconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_title_crew_name_basics1`
    FOREIGN KEY (`director`)
    REFERENCES `imdb`.`name_basics` (`nconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `imdb`.`title_episode`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`title_episode` ;

CREATE TABLE IF NOT EXISTS `imdb`.`title_episode` (
  `tconst` INT NOT NULL,
  `parentTconst` INT NOT NULL,
  `seasonNumber` VARCHAR(45) NULL,
  `episodeNumber` VARCHAR(45) NULL,
  INDEX `fk_title_episode_title_basics2_idx` (`parentTconst` ASC),
  CONSTRAINT `fk_title_episode_title_basics1`
    FOREIGN KEY (`tconst`)
    REFERENCES `imdb`.`title_basics` (`tconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_title_episode_title_basics2`
    FOREIGN KEY (`parentTconst`)
    REFERENCES `imdb`.`title_basics` (`tconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `imdb`.`title_principals`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`title_principals` ;

CREATE TABLE IF NOT EXISTS `imdb`.`title_principals` (
  `tconst` INT NOT NULL,
  `ordering` INT NULL,
  `nconst` INT NOT NULL,
  `category` VARCHAR(20) NULL,
  `job` VARCHAR(290) NULL,
  `characters` VARCHAR(470) NULL,
  INDEX `fk_title_principals_name_basics1_idx` (`nconst` ASC),
  CONSTRAINT `fk_title_principals_title_basics1`
    FOREIGN KEY (`tconst`)
    REFERENCES `imdb`.`title_basics` (`tconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_title_principals_name_basics1`
    FOREIGN KEY (`nconst`)
    REFERENCES `imdb`.`name_basics` (`nconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `imdb`.`title_ratings`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`title_ratings` ;

CREATE TABLE IF NOT EXISTS `imdb`.`title_ratings` (
  `tconst` INT NOT NULL,
  `averageRating` FLOAT NULL,
  `numVotes` INT NULL,
  INDEX `fk_title_ratings_title_basics1_idx` (`tconst` ASC),
  CONSTRAINT `fk_title_ratings_title_basics1`
    FOREIGN KEY (`tconst`)
    REFERENCES `imdb`.`title_basics` (`tconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `imdb`.`title_crew_writer`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imdb`.`title_crew_writer` ;

CREATE TABLE IF NOT EXISTS `imdb`.`title_crew_writer` (
  `tconst` INT NOT NULL,
  `writer` INT NOT NULL,
  INDEX `fk_title_crew_title_basics1_idx` (`tconst` ASC),
  INDEX `fk_title_crew_name_basics2_idx` (`writer` ASC),
  CONSTRAINT `fk_title_crew_title_basics10`
    FOREIGN KEY (`tconst`)
    REFERENCES `imdb`.`title_basics` (`tconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_title_crew_name_basics20`
    FOREIGN KEY (`writer`)
    REFERENCES `imdb`.`name_basics` (`nconst`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
