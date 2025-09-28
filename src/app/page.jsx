import Image from "next/image";
import styles from "./page.module.css";
import LeftSearchBar from "./components/leftSearchBar/LeftSearchBar";
import Map from "./components/map/Map"
import Icons from "./components/IconsContainer/Icons";

export default function Home() {
  return (
    <div className={styles.container}>
      <div className={styles.LeftSearchBar}>
        <LeftSearchBar />
      </div>
      <div className={styles.Icons}>
        <Icons />
      </div>
      <div className={styles.Map}>
        <Map />
      </div>
    </div>
  );
}